/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.adaptive

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

import org.apache.spark.MapOutputStatistics
import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate
import org.apache.spark.util.ThreadUtils

/**
 * In adaptive execution mode, an execution plan is divided into multiple QueryStages. Each
 * QueryStage is a sub-tree that runs in a single stage.
 */
abstract class QueryStage extends UnaryExecNode {

  var child: SparkPlan

  protected var _mapOutputStatistics: MapOutputStatistics = null

  def mapOutputStatistics: MapOutputStatistics = _mapOutputStatistics

  // Ignore this wrapper for canonicalizing.
  override lazy val canonicalized: SparkPlan = child.canonicalized

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  def executeChildStages(): Unit = {
    // Handle broadcast stages
    val broadcastQueryStages: Seq[BroadcastQueryStage] = child.collect {
      case BroadcastQueryStageInput(queryStage: BroadcastQueryStage, _) => queryStage
    }
    val broadcastFutures = broadcastQueryStages.map { queryStage =>
      Future { queryStage.prepareBroadcast() }(ThreadUtils.sameThread)
    }

    // Submit shuffle stages
    val executionId = sqlContext.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val shuffleQueryStages: Seq[ShuffleQueryStage] = child.collect {
      case ShuffleQueryStageInput(queryStage: ShuffleQueryStage, _, _, _, _, _) => queryStage
    }
    val shuffleStageFutures = shuffleQueryStages.map { queryStage =>
      Future {
        SQLExecution.withExecutionId(sqlContext.sparkContext, executionId) {
          queryStage.execute()
        }
      }(QueryStage.executionContext)
    }

    (broadcastFutures ++ shuffleStageFutures).foreach { future =>
      ThreadUtils.awaitResult(future, Duration.Inf)
    }
  }

  def executeStage(): RDD[InternalRow] = child.execute()

  private var cachedRDD: Option[RDD[InternalRow]] = None
  private var cachedArray: Option[Array[InternalRow]] = None

  def doPreExecutionOptimization(): Unit = {
    // 1. Execute childStages and optimize the plan in this stage
    executeChildStages()

    // Optimize join in this stage based on previous stages' statistics.
    val oldChild = child
    OptimizeJoin(conf).apply(this)
    HandleSkewedJoin(conf).apply(this)
    // If the Joins are changed, we need apply EnsureRequirements rule to add BroadcastExchange.
    if (!oldChild.fastEquals(child)) {
      child = EnsureRequirements(conf).apply(child)
    }

    // 2. Determine reducer number
    val queryStageInputs: Seq[ShuffleQueryStageInput] = child.collect {
      case input: ShuffleQueryStageInput if !input.isLocalShuffle => input
    }
    val childMapOutputStatistics = queryStageInputs.map(_.childStage.mapOutputStatistics)
      .filter(_ != null).toArray
    if (childMapOutputStatistics.length > 0) {

      val exchangeCoordinator = new ExchangeCoordinator(
        conf.targetPostShuffleInputSize,
        conf.adaptiveTargetPostShuffleRowCount,
        conf.minNumPostShufflePartitions)

      if (queryStageInputs.length == 2 && queryStageInputs.forall(_.skewedPartitions.isDefined)) {
        // If a skewed join is detected and optimized, we will omit the skewed partitions when
        // estimate the partition start and end indices.
        val (partitionStartIndices, partitionEndIndices) =
          exchangeCoordinator.estimatePartitionStartEndIndices(
            childMapOutputStatistics, queryStageInputs(0).skewedPartitions.get)
        queryStageInputs.foreach { i =>
          i.partitionStartIndices = Some(partitionStartIndices)
          i.partitionEndIndices = Some(partitionEndIndices)
        }
      } else {
        val partitionStartIndices =
          exchangeCoordinator.estimatePartitionStartIndices(childMapOutputStatistics)
        queryStageInputs.foreach(_.partitionStartIndices = Some(partitionStartIndices))
      }
    }

    // 3. Codegen and update the UI
    child = CollapseCodegenStages(sqlContext.conf).apply(child)
    val executionId = sqlContext.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionId != null && executionId.nonEmpty) {
      val queryExecution = SQLExecution.getQueryExecution(executionId.toLong)
      sparkContext.listenerBus.post(SparkListenerSQLAdaptiveExecutionUpdate(
        executionId.toLong,
        queryExecution.toString,
        SparkPlanInfo.fromSparkPlan(queryExecution.executedPlan)))
    }
  }

  override def doExecute(): RDD[InternalRow] = synchronized {
    cachedRDD match {
      case None =>
        doPreExecutionOptimization()
        cachedRDD = Some(executeStage())
      case Some(cached) =>
    }
    cachedRDD.get
  }

  override def executeCollect(): Array[InternalRow] = synchronized {
    cachedArray match {
      case None =>
        doPreExecutionOptimization()
        cachedArray = Some(child.executeCollect())
      case Some(cached) =>
    }
    cachedArray.get
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      builder: StringBuilder,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false): StringBuilder = {
    child.generateTreeString(depth, lastChildren, builder, verbose, "*")
  }
}

object QueryStage {
  private[execution] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("adaptive-query-stage"))
}

/** The last QueryStage of an execution plan. */
case class ResultQueryStage(var child: SparkPlan) extends QueryStage

/** A shuffle QueryStage whose child is a ShuffleExchange. */
case class ShuffleQueryStage(var child: SparkPlan) extends QueryStage {
  override def executeStage(): RDD[InternalRow] = {
    child match {
      case e: ShuffleExchange =>
        val result = e.eagerExecute()
        _mapOutputStatistics = e.mapOutputStatistics
        result
      case _ => throw new IllegalArgumentException(
        "The child of ShuffleQueryStage must be a ShuffleExchange.")
    }
  }
}

/** A broadcast QueryStage whose child is a BroadcastExchangeExec. */
case class BroadcastQueryStage(var child: SparkPlan) extends QueryStage {
  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.executeBroadcast()
  }

  private var prepared = false

  def prepareBroadcast() : Unit = synchronized {
    if (!prepared) {
      executeChildStages()
      child = CollapseCodegenStages(sqlContext.conf).apply(child)
      // After child stages are completed, prepare() triggers the broadcast.
      prepare()
      prepared = true
    }
  }

  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "BroadcastExchange does not support the execute() code path.")
  }
}
