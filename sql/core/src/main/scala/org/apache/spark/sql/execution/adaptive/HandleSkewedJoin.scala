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

import scala.collection.immutable.Nil
import scala.collection.mutable

import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SortExec, SparkPlan, UnionExec}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.execution.statsEstimation.PartitionStatistics
import org.apache.spark.sql.internal.SQLConf

case class HandleSkewedJoin(conf: SQLConf) extends Rule[SparkPlan] {

  private val supportedJoinTypes = Inner :: Cross :: LeftSemi :: LeftOuter:: RightOuter :: Nil

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
   * spark.sql.adaptive.skewedPartitionRowCountThreshold.
   */
  private def isSkewed(
      stats: PartitionStatistics,
      partitionId: Int,
      medianSize: Long,
      medianRowCount: Long): Boolean = {
    isSizeSkewed(stats.bytesByPartitionId(partitionId), medianSize) ||
      isRowCountSkewed(stats.recordsByPartitionId(partitionId), medianRowCount)
  }

  private def medianSizeAndRowCount(stats: PartitionStatistics): (Long, Long) = {
    val bytesLen = stats.bytesByPartitionId.length
    val rowCountsLen = stats.recordsByPartitionId.length
    val bytes = stats.bytesByPartitionId.sorted
    val rowCounts = stats.recordsByPartitionId.sorted
    val medSize = if (bytes(bytesLen / 2) > 0) bytes(bytesLen / 2) else 1
    val medRowCount = if (rowCounts(rowCountsLen / 2) > 0) rowCounts(rowCountsLen / 2) else 1
    (medSize, medRowCount)
  }

  /**
   * To equally divide n elements into m buckets, basically each bucket should have n/m elements,
   * for the remaining n%m elements, add one more element to the first n%m buckets each. Returns
   * a sequence with length numBuckets and each value represents the start index of each bucket.
   */
  def equallyDivide(numElements: Int, numBuckets: Int): Seq[Int] = {
    val elementsPerBucket = numElements / numBuckets
    val remaining = numElements % numBuckets
    val splitPoint = (elementsPerBucket + 1) * remaining
    (0 until remaining).map(_ * (elementsPerBucket + 1)) ++
      (remaining until numBuckets).map(i => splitPoint + (i - remaining) * elementsPerBucket)
  }

  /**
   * We split the partition into several splits. Each split reads the data from several map outputs
   * ranging from startMapId to endMapId(exclusive). This method calculates the split number and
   * the startMapId for all splits.
   */
  private def estimateMapIdStartIndices(
      queryStageInput: ShuffleQueryStageInput,
      partitionId: Int,
      medianSize: Long,
      medianRowCount: Long): Array[Int] = {
    val stats = queryStageInput.childStage.stats
    val size = stats.bytesByPartitionId.get(partitionId)
    val rowCount = stats.recordStatistics.get.recordsByPartitionId(partitionId)
    val factor = Math.max(size / medianSize, rowCount / medianRowCount)
    val numSplits = Math.min(conf.adaptiveSkewedMaxSplits,
      Math.min(factor.toInt, queryStageInput.numMapper))
    equallyDivide(queryStageInput.numMapper, numSplits).toArray
  }

  /**
   * Base optimization support check: the join type is supported and plan statistics is available.
   * Note that for some join types(like left outer), whether a certain partition can be optimized
   * also depends on the filed isSkewAndSupportsSplit.
   */
  private def supportOptimization(
      joinType: JoinType,
      left: QueryStageInput,
      right: QueryStageInput): Boolean = {
      supportedJoinTypes.contains(joinType) &&
      left.childStage.stats.getPartitionStatistics.isDefined &&
      right.childStage.stats.getPartitionStatistics.isDefined
  }

  private def supportSplitOnLeftPartition(joinType: JoinType) = joinType != RightOuter

  private def supportSplitOnRightPartition(joinType: JoinType) = {
    joinType != LeftOuter && joinType != LeftSemi
  }

  private def handleSkewedJoin(
      operator: SparkPlan,
      queryStage: QueryStage): SparkPlan = operator.transformUp {
    case smj @ SortMergeJoinExec(leftKeys, rightKeys, joinType, condition,
      SortExec(_, _, left: ShuffleQueryStageInput, _),
      SortExec(_, _, right: ShuffleQueryStageInput, _))
      if supportOptimization(joinType, left, right) =>

      val leftStats = left.childStage.stats.getPartitionStatistics.get
      val rightStats = right.childStage.stats.getPartitionStatistics.get
      val numPartitions = leftStats.bytesByPartitionId.length
      val (leftMedSize, leftMedRowCount) = medianSizeAndRowCount(leftStats)
      val (rightMedSize, rightMedRowCount) = medianSizeAndRowCount(rightStats)
      logInfo(s"HandlingSkewedJoin left medSize/rowCounts: ($leftMedSize, $leftMedRowCount)" +
        s" right medSize/rowCounts ($rightMedSize, $rightMedRowCount)")

      logInfo(s"left bytes Max : ${leftStats.bytesByPartitionId.max}")
      logInfo(s"left row counts Max : ${leftStats.recordsByPartitionId.max}")
      logInfo(s"right bytes Max : ${rightStats.bytesByPartitionId.max}")
      logInfo(s"right row counts Max : ${rightStats.recordsByPartitionId.max}")

      val skewedPartitions = mutable.HashSet[Int]()
      val subJoins = mutable.ArrayBuffer[SparkPlan](smj)
      for (partitionId <- 0 until numPartitions) {
        val isLeftSkew = isSkewed(leftStats, partitionId, leftMedSize, leftMedRowCount)
        val isRightSkew = isSkewed(rightStats, partitionId, rightMedSize, rightMedRowCount)
        val isSkewAndSupportsSplit =
          (isLeftSkew && supportSplitOnLeftPartition(joinType)) ||
            (isRightSkew && supportSplitOnRightPartition(joinType))

        if (isSkewAndSupportsSplit) {
          skewedPartitions += partitionId
          val leftMapIdStartIndices = if (isLeftSkew && supportSplitOnLeftPartition(joinType)) {
            estimateMapIdStartIndices(left, partitionId, leftMedSize, leftMedRowCount)
          } else {
            Array(0)
          }
          val rightMapIdStartIndices = if (isRightSkew && supportSplitOnRightPartition(joinType)) {
            estimateMapIdStartIndices(right, partitionId, rightMedSize, rightMedRowCount)
          } else {
            Array(0)
          }

          for (i <- 0 until leftMapIdStartIndices.length;
               j <- 0 until rightMapIdStartIndices.length) {
            val leftEndMapId = if (i == leftMapIdStartIndices.length - 1) {
              left.numMapper
            } else {
              leftMapIdStartIndices(i + 1)
            }
            val rightEndMapId = if (j == rightMapIdStartIndices.length - 1) {
              right.numMapper
            } else {
              rightMapIdStartIndices(j + 1)
            }

            val leftInput =
              SkewedShuffleQueryStageInput(
                left.childStage, left.output, partitionId, leftMapIdStartIndices(i), leftEndMapId)
            val rightInput =
              SkewedShuffleQueryStageInput(
                right.childStage, right.output, partitionId,
                rightMapIdStartIndices(j), rightEndMapId)

            subJoins +=
              SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, leftInput, rightInput)
          }
        }
      }
      logInfo(s"skewed partition number is ${skewedPartitions.size}")
      if (skewedPartitions.size > 0) {
        left.skewedPartitions = Some(skewedPartitions)
        right.skewedPartitions = Some(skewedPartitions)
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
