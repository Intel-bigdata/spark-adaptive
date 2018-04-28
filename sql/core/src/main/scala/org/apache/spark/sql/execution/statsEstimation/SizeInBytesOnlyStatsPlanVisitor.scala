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

package org.apache.spark.sql.execution.statsEstimation

import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStage
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{HashJoin, SortMergeJoinExec}

object SizeInBytesOnlyStatsPlanVisitor extends SparkPlanVisitor[Statistics] {

  private def visitUnaryExecNode(p: UnaryExecNode): Statistics = {
    // There should be some overhead in Row object, the size should not be zero when there is
    // no columns, this help to prevent divide-by-zero error.
    val childRowSize = p.child.output.map(_.dataType.defaultSize).sum + 8
    val outputRowSize = p.output.map(_.dataType.defaultSize).sum + 8
    // Assume there will be the same number of rows as child has.
    var sizeInBytes = (p.child.stats.sizeInBytes * outputRowSize) / childRowSize
    if (sizeInBytes == 0) {
      // sizeInBytes can't be zero, or sizeInBytes of BinaryNode will also be zero
      // (product of children).
      sizeInBytes = 1
    }

    // Don't propagate rowCount and attributeStats, since they are not estimated here.
    Statistics(sizeInBytes = sizeInBytes)
  }

  override def default(p: SparkPlan): Statistics = p match {
    case p: LeafExecNode => p.computeStats()
    case _: SparkPlan => Statistics(
      sizeInBytes = p.children.map(_.stats.sizeInBytes).product,
      rowCount = Some(p.children.map(_.stats.rowCount.getOrElse(BigInt(1))).product))
  }

  override def visitFilterExec(p: FilterExec): Statistics = visitUnaryExecNode(p)

  override def visitProjectExec(p: ProjectExec): Statistics = visitUnaryExecNode(p)

  override def visitHashAggregateExec(p: HashAggregateExec): Statistics = {
    if (p.groupingExpressions.isEmpty) {
      val sizeInBytes = 8 + p.output.map(_.dataType.defaultSize).sum
      Statistics(sizeInBytes)
    } else {
      visitUnaryExecNode(p)
    }
  }

  override def visitHashJoin(p: HashJoin): Statistics = {
    p.joinType match {
      case LeftAnti | LeftSemi =>
        // LeftSemi and LeftAnti won't ever be bigger than left
        p.left.stats
      case _ =>
        Statistics(sizeInBytes = p.left.stats.sizeInBytes * p.right.stats.sizeInBytes)
    }
  }

  override def visitShuffleExchangeExec(p: ShuffleExchangeExec): Statistics = {
    if (p.mapOutputStatistics != null) {
      val sizeInBytes = p.mapOutputStatistics.bytesByPartitionId.sum
      val bytesByPartitionId = p.mapOutputStatistics.bytesByPartitionId
      if (p.mapOutputStatistics.recordsByPartitionId.nonEmpty) {
        val record = p.mapOutputStatistics.recordsByPartitionId.sum
        val recordsByPartitionId = p.mapOutputStatistics.recordsByPartitionId
        Statistics(sizeInBytes = sizeInBytes,
          rowCount = Some(record),
          bytesByPartitionId = Some(bytesByPartitionId),
          recordStatistics = Some(RecordStatistics(record, recordsByPartitionId)))
      } else {
        Statistics(sizeInBytes = sizeInBytes, bytesByPartitionId = Some(bytesByPartitionId))
      }
    } else {
      visitUnaryExecNode(p)
    }
  }

  override def visitSortAggregateExec(p: SortAggregateExec): Statistics = {
    if (p.groupingExpressions.isEmpty) {
      val sizeInBytes = 8 + p.output.map(_.dataType.defaultSize).sum
      Statistics(sizeInBytes)
    } else {
      visitUnaryExecNode(p)
    }
  }

  override def visitSortMergeJoinExec(p: SortMergeJoinExec): Statistics = {
    p.joinType match {
      case LeftAnti | LeftSemi =>
        // LeftSemi and LeftAnti won't ever be bigger than left
        p.left.stats
      case _ =>
        default(p)
    }
  }

  override def visitShuffleQueryStage(p: ShuffleQueryStage): Statistics = {
    if (p.mapOutputStatistics != null) {
      val sizeInBytes = p.mapOutputStatistics.bytesByPartitionId.sum
      val bytesByPartitionId = p.mapOutputStatistics.bytesByPartitionId
      if (p.mapOutputStatistics.recordsByPartitionId.nonEmpty) {
        val record = p.mapOutputStatistics.recordsByPartitionId.sum
        val recordsByPartitionId = p.mapOutputStatistics.recordsByPartitionId
        Statistics(sizeInBytes = sizeInBytes,
          rowCount = Some(record),
          bytesByPartitionId = Some(bytesByPartitionId),
          recordStatistics = Some(RecordStatistics(record, recordsByPartitionId)))
      } else {
        Statistics(sizeInBytes = sizeInBytes, bytesByPartitionId = Some(bytesByPartitionId))
      }
    } else {
      visitUnaryExecNode(p)
    }
  }
}
