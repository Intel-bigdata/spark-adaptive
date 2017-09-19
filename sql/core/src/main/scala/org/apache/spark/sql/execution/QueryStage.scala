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

package org.apache.spark.sql.execution

import java.util.concurrent.Future

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.MapOutputStatistics
import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BuildLeft, BuildRight, SortMergeJoinExec}
import org.apache.spark.sql.execution.statsEstimation.{SizeInBytesOnlyStatsPlanVisitor, Statistics}
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ThreadUtils

/**
 * QueryStageInput is the leaf node of a QueryStage and is used to hide its child stage.
 */
abstract class QueryStageInput extends LeafExecNode {

  def childStage: QueryStage

  // Ignore this wrapper for canonicalizing.
  override lazy val canonicalized: SparkPlan = childStage.canonicalized

  // `QueryStageInput` can have distinct set of output attribute ids from its childStage, we need
  // to update the attribute ids in `outputPartitioning` and `outputOrdering`.
  private lazy val updateAttr: Expression => Expression = {
    val originalAttrToNewAttr = AttributeMap(childStage.output.zip(output))
    e => e.transform {
      case attr: Attribute => originalAttrToNewAttr.getOrElse(attr, attr)
    }
  }

  override def outputPartitioning: Partitioning = childStage.outputPartitioning match {
    case h: HashPartitioning => h.copy(expressions = h.expressions.map(updateAttr))
    case other => other
  }

  override def outputOrdering: Seq[SortOrder] = {
    childStage.outputOrdering.map(updateAttr(_).asInstanceOf[SortOrder])
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      builder: StringBuilder,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false): StringBuilder = {
    childStage.generateTreeString(depth, lastChildren, builder, verbose, "*")
  }

  override def computeStats: Statistics = {
    childStage.stats
  }
}

case class ShuffleQueryStageInput(
    childStage: QueryStage,
    override val output: Seq[Attribute],
    var isLocalShuffle: Boolean = false,
    // TODO refine this later
    var isAdaptiveShuffle: Boolean = false,
    var skewedPartitions: Option[mutable.HashSet[Int]] = None,
    var partitionStartIndices: Option[Array[Int]] = None,
    var partitionEndIndices: Option[Array[Int]] = None)
  extends QueryStageInput {

  override def outputPartitioning: Partitioning = partitionStartIndices.map {
    indices => UnknownPartitioning(indices.length)
  }.getOrElse(super.outputPartitioning)

  override def doExecute(): RDD[InternalRow] = {
    val childRDD = childStage.execute().asInstanceOf[ShuffledRowRDD]
    if (isLocalShuffle) {
      new LocalShuffledRowRDD(childRDD.dependency, partitionStartIndices, partitionEndIndices)
    } else if (isAdaptiveShuffle) {
      assert(partitionStartIndices.isDefined && partitionEndIndices.isDefined &&
        partitionEndIndices.get(0) == partitionStartIndices.get(0) + 1)
      new AdaptiveShuffledRowRDD(childRDD.dependency, partitionStartIndices.get(0))
    } else {
      new ShuffledRowRDD(childRDD.dependency, partitionStartIndices, partitionEndIndices)
    }
  }
}

case class BroadcastQueryStageInput(
    childStage: QueryStage,
    override val output: Seq[Attribute])
  extends QueryStageInput {

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    childStage.executeBroadcast()
  }

  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "BroadcastStageInput does not support the execute() code path.")
  }
}

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
    // Execute childStages. Use a thread pool to avoid blocking on one child stage.
    val executionId = sqlContext.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)

    val queryStageSubmitTasks = mutable.ArrayBuffer[Future[_]]()

    // Handle broadcast stages
    val broadcastQueryStages: Seq[BroadcastQueryStage] = child.collect {
      case BroadcastQueryStageInput(queryStage: BroadcastQueryStage, _) => queryStage
    }
    broadcastQueryStages.foreach { queryStage =>
      queryStageSubmitTasks += QueryStage.queryStageThreadPool.submit(
        new Runnable {
          override def run(): Unit = {
            queryStage.prepareBroadcast()
          }
        })
    }

    // Submit shuffle stages
    val shuffleQueryStages: Seq[ShuffleQueryStage] = child.collect {
      case ShuffleQueryStageInput(queryStage: ShuffleQueryStage, _, _, _, _, _, _) => queryStage
    }
    shuffleQueryStages.foreach { queryStage =>
      queryStageSubmitTasks += QueryStage.queryStageThreadPool.submit(
        new Runnable {
          override def run(): Unit = {
            SQLExecution.withExecutionId(sqlContext.sparkContext, executionId) {
              queryStage.execute()
            }
          }
        })
    }

    queryStageSubmitTasks.foreach(_.get())
  }

  def executeStage(): RDD[InternalRow] = child.execute()

  private var cachedRDD: RDD[InternalRow] = null

  override def doExecute(): RDD[InternalRow] = synchronized {
    if (cachedRDD == null) {
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
        case input: ShuffleQueryStageInput if (!input.partitionStartIndices.isDefined) => input
      }
      val childMapOutputStatistics = queryStageInputs.map(_.childStage.mapOutputStatistics)
        .filter(_ != null).toArray
      if (childMapOutputStatistics.length > 0) {
        val minNumPostShufflePartitions =
          if (conf.minNumPostShufflePartitions > 0) Some(conf.minNumPostShufflePartitions) else None

        val exchangeCoordinator = new ExchangeCoordinator(
          conf.targetPostShuffleInputSize,
          minNumPostShufflePartitions)

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

      // 4. Execute the plan in this stage
      cachedRDD = executeStage()
    }
    cachedRDD
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
  lazy val queryStageThreadPool =
    ThreadUtils.newDaemonCachedThreadPool("adaptive-query-stage-pool")
}

case class ResultQueryStage(var child: SparkPlan) extends QueryStage

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

case class OptimizeJoin(conf: SQLConf) extends Rule[SparkPlan] {

  private def canBuildRight(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | LeftOuter | LeftSemi | LeftAnti => true
    case j: ExistenceJoin => true
    case _ => false
  }

  private def canBuildLeft(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | RightOuter => true
    case _ => false
  }

  private def canBroadcast(plan: SparkPlan): Boolean = {
    plan.stats.sizeInBytes >= 0 && plan.stats.sizeInBytes <= conf.adaptiveBroadcastJoinThreshold
  }

  private def removeSort(plan: SparkPlan): SparkPlan = {
    plan match {
      case s: SortExec => s.child
      case p: SparkPlan => p
    }
  }

  // Change to functional way?
  private def calculatePartitionStartEndIndices(plan: QueryStage): (Array[Int], Array[Int]) = {
    val rowStatisticsByPartitionId = SizeInBytesOnlyStatsPlanVisitor.visitQueryStage(
      plan).partStatistics.get.rowsByPartitionId
    val partitionStartIndicies = ArrayBuffer[Int]()
    val partitionEndIndicies = ArrayBuffer[Int]()
    var continuousZeroFlag = false
    var end = 0
    var i = 0
    for (rows <- rowStatisticsByPartitionId) {
      if (rows != 0) {
        if (!continuousZeroFlag) {
          partitionStartIndicies += i
          continuousZeroFlag = true
        }
      } else {
        if (continuousZeroFlag) {
          partitionEndIndicies += i
          continuousZeroFlag = false
        }
      }
      i += 1
    }
    if (continuousZeroFlag) {
      partitionEndIndicies += i
    }
    (partitionStartIndicies.toArray, partitionEndIndicies.toArray)
  }

  private def optimizeSortMergeJoin(
      smj: SortMergeJoinExec,
      queryStage: QueryStage): SparkPlan = {
    smj match {
      case SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right) =>
        val broadcastSide = if (canBuildRight(joinType) && canBroadcast(right)) {
          Some(BuildRight)
        } else if (canBuildLeft(joinType) && canBroadcast(left)) {
          Some(BuildLeft)
        } else {
          None
        }
      broadcastSide.map { buildSide =>
        val broadcastJoin = BroadcastHashJoinExec(
          leftKeys, rightKeys, joinType, buildSide, condition, removeSort(left), removeSort(right))

        val newChild = queryStage.child.transformDown {
          case s: SortMergeJoinExec if (s.fastEquals(smj)) => broadcastJoin
        }
        // Apply EnsureRequirement rule to check if any new Exchange will be added. If no
        // Exchange is added, we convert the sortMergeJoin to BroadcastHashJoin. Otherwise
        // we don't convert it because it causes additional Shuffle.
        val afterEnsureRequirements = EnsureRequirements(conf).apply(newChild)
        val numExchanges = afterEnsureRequirements.collect {
          case e: ShuffleExchange => e
        }.length

        if ((numExchanges == 0) ||
          (queryStage.isInstanceOf[ShuffleQueryStage] && numExchanges <= 1)) {
          val broadcastSidePlan = if (broadcastSide.get.equals(BuildLeft)) {
            // TODO RemoveSort(left) may not return a QueryStageInput?
            removeSort(left).asInstanceOf[QueryStageInput].childStage
          } else {
            removeSort(right).asInstanceOf[QueryStageInput].childStage
          }
          // Not read the partitions which has 0 rows on build side
          val startAndEndIndices = calculatePartitionStartEndIndices(
            broadcastSidePlan.asInstanceOf[QueryStage])

          // Set QueryStageInput to return local shuffled RDD
          broadcastJoin.children.foreach {
            case input: ShuffleQueryStageInput =>
              input.isLocalShuffle = true
              input.partitionStartIndices = Some(startAndEndIndices._1)
              input.partitionEndIndices = Some(startAndEndIndices._2)
            case _ =>
          }
          // Update the plan in queryStage
          queryStage.child = newChild
          broadcastJoin
        } else {
          smj
        }
      }.getOrElse(smj)
    }
  }

  private def optimizeJoin(
      operator: SparkPlan,
      queryStage: QueryStage): SparkPlan = {
    operator match {
      case smj: SortMergeJoinExec =>
        val op = optimizeSortMergeJoin(smj, queryStage)
        val optimizedChildren = op.children.map(optimizeJoin(_, queryStage))
        op.withNewChildren(optimizedChildren)
      case op =>
        val optimizedChildren = op.children.map(optimizeJoin(_, queryStage))
        op.withNewChildren(optimizedChildren)
    }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.adaptiveJoinEnabled) {
      plan
    } else {
      plan match {
        case queryStage: QueryStage =>
          val optimizedPlan = optimizeJoin(queryStage.child, queryStage)
          queryStage.child = optimizedPlan
          queryStage
        case _ => plan
      }
    }
  }
}

case class HandleSkewedJoin(conf: SQLConf) extends Rule[SparkPlan] {

  private def isSizeSkewed(size: Long, medianSize: Long): Boolean = {
    size > medianSize * conf.adaptiveSkewedFactor &&
      size > conf.adaptiveSkewedSizeThreshold
  }

  private def isRowCountSkewed(rowCount: Long, medianRowCount: Long): Boolean = {
    rowCount > medianRowCount * conf.adaptiveSkewedFactor &&
      rowCount > conf.adaptiveSkewedRowCountThreshold
  }

  /**
   * A partition is considered as a skewed partition if its size is larger than the median
   * partition size * spark.sql.adaptive.skewedPartitionFactor and also larger than
   * spark.sql.adaptive.skewedPartitionSizeThreshold, or if its row count is larger than
   * the median row count * spark.sql.adaptive.skewedPartitionFactor and also larger than
   * spark.sql.adaptive.skewedPartitionSizeThreshold.
   */
  private def skewedPartitions(plan: QueryStage): Seq[Int] = {
    plan.stats.partStatistics match {
      case Some(partitionStats) =>
        val bytesByPartitionId = partitionStats.bytesByPartitionId
        val rowsByPartitionId = partitionStats.rowsByPartitionId
        val sortedBytesByPartitionId = bytesByPartitionId.sorted
        val sortedRowsByPartitionId = rowsByPartitionId.sorted
        val medianPartitionSize = sortedBytesByPartitionId(bytesByPartitionId.length / 2)
        val medianRowCount = sortedRowsByPartitionId(rowsByPartitionId.length / 2)

        val skewedPartitions = new ArrayBuffer[Int]()
        for (i <- 0 until bytesByPartitionId.length) {
          if (isSizeSkewed(bytesByPartitionId(i), medianPartitionSize) ||
            isRowCountSkewed(rowsByPartitionId(i), medianRowCount)) {
            skewedPartitions += i
          }
        }
        skewedPartitions.toArray
      case None => Seq.empty[Int]
    }
  }

  private def canBroadcast(plan: SparkPlan, partitionId: Int): Boolean = {
    plan.stats.partStatistics match {
      case Some(partitionStats) =>
        partitionStats.bytesByPartitionId(partitionId) >= 0 &&
        partitionStats.bytesByPartitionId(partitionId) <= conf.adaptiveBroadcastJoinThreshold
      case None => false
    }
  }

  private def handleSkewedJoin(
      operator: SparkPlan,
      queryStage: QueryStage): SparkPlan = operator.transformUp {
    case smj @ SortMergeJoinExec(leftKeys, rightKeys, joinType, condition,
      SortExec(_, _, left: ShuffleQueryStageInput, _),
      SortExec(_, _, right: ShuffleQueryStageInput, _)) =>
      val skewedPartitionsInLeft = skewedPartitions(left.childStage)
      val skewedPartitionsInRight = skewedPartitions(right.childStage)
      val handledPartitions = mutable.HashSet[Int]()
      val subJoins = mutable.ArrayBuffer[SparkPlan](smj)
      skewedPartitionsInLeft.foreach { p =>
        if (canBroadcast(right, p)) {
          handledPartitions += p
          val leftInput =
            ShuffleQueryStageInput(
              left.childStage,
              left.output,
              isAdaptiveShuffle = true,
              partitionStartIndices = Some(Array(p)),
              partitionEndIndices = Some(Array(p + 1)))
          val rightInput =
            ShuffleQueryStageInput(
              right.childStage,
              right.output,
              isAdaptiveShuffle = true,
              partitionStartIndices = Some(Array(p)),
              partitionEndIndices = Some(Array(p + 1)))
          subJoins += BroadcastHashJoinExec(
            leftKeys, rightKeys, joinType, joins.BuildRight, condition, leftInput, rightInput)
        }
      }
      skewedPartitionsInRight.foreach { p =>
        if (!handledPartitions.contains(p) && canBroadcast(left, p)) {
          handledPartitions += p
          val leftInput =
            ShuffleQueryStageInput(
              left.childStage,
              left.output,
              partitionStartIndices = Some(Array(p)),
              partitionEndIndices = Some(Array(p + 1)))
          val rightInput =
            ShuffleQueryStageInput(
              right.childStage,
              right.output,
              partitionStartIndices = Some(Array(p)),
              partitionEndIndices = Some(Array(p + 1)))
          subJoins += BroadcastHashJoinExec(
            leftKeys, rightKeys, joinType, joins.BuildLeft, condition, leftInput, rightInput)
        }
      }
      if (handledPartitions.size > 0) {
        left.skewedPartitions = Some(handledPartitions)
        right.skewedPartitions = Some(handledPartitions)
        UnionExec(subJoins.toList)
      } else {
        smj
      }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.adaptiveSkewedJoinEnabled) {
      plan
    } else {
      plan match {
        case queryStage: QueryStage =>
          val queryStageInputs: Seq[ShuffleQueryStageInput] = queryStage.collect {
            case input: ShuffleQueryStageInput => input
          }
          if (queryStageInputs.length == 2) {
            // Currently we only support handling skewed join for 2 table join.
            val optimizedPlan = handleSkewedJoin(queryStage.child, queryStage)
            queryStage.child = optimizedPlan
            queryStage
          } else {
            queryStage
          }
        case _ => plan
      }
    }
  }
}

case class PlanQueryStage(conf: SQLConf) extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.adaptiveExecutionEnabled) {
      return plan
    }

    // Build a hash map using schema of exchanges to avoid O(N*N) sameResult calls.
    val stages = mutable.HashMap[StructType, ArrayBuffer[QueryStage]]()

    val newPlan = plan.transformUp {
      case exchange: Exchange =>
        val sameSchema = stages.getOrElseUpdate(exchange.schema, ArrayBuffer[QueryStage]())
        val samePlan = sameSchema.find { s =>
          exchange.sameResult(s.child)
        }
        if (samePlan.isDefined) {
          // Keep the output of this exchange, the following plans require that to resolve
          // attributes.
          exchange match {
            case e: ShuffleExchange =>
              ShuffleQueryStageInput(samePlan.get, exchange.output)
            case e: BroadcastExchangeExec =>
              BroadcastQueryStageInput(samePlan.get, exchange.output)
          }
        } else {
          val queryStageInput = exchange match {
            case e: ShuffleExchange =>
              ShuffleQueryStageInput(ShuffleQueryStage(e), e.output)
            case e: BroadcastExchangeExec =>
              BroadcastQueryStageInput(BroadcastQueryStage(e), e.output)
          }
          sameSchema += queryStageInput.childStage
          queryStageInput
        }
    }
    newPlan match {
      case c: ExecutedCommandExec => c
      case other => ResultQueryStage(other)
    }
  }
}
