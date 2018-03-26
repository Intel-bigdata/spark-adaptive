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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ShuffleExchange}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BuildLeft, BuildRight, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

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

  private[adaptive] def calculatePartitionStartEndIndices(
      bytesByPartitionId: Array[Long]): (Array[Int], Array[Int]) = {
    val partitionStartIndices = ArrayBuffer[Int]()
    val partitionEndIndices = ArrayBuffer[Int]()
    var continuousZeroFlag = false
    var i = 0
    for (bytes <- bytesByPartitionId) {
      if (bytes != 0 && !continuousZeroFlag) {
        partitionStartIndices += i
        continuousZeroFlag = true
      } else if (bytes == 0 && continuousZeroFlag) {
        partitionEndIndices += i
        continuousZeroFlag = false
      }
      i += 1
    }
    if (continuousZeroFlag) {
      partitionEndIndices += i
    }
    if (partitionStartIndices.length == 0) {
      (Array(0), Array(0))
    } else {
      (partitionStartIndices.toArray, partitionEndIndices.toArray)
    }
  }

  // After transforming to BroadcastJoin from SortMergeJoin, local shuffle read should be used and
  // there's opportunity to read less partitions based on previous shuffle write results.
  private def optimizeForLocalShuffleReadLessPartitions(
      joinType: JoinType,
      broadcastSidePlan: SparkPlan,
      childrenPlans: Seq[SparkPlan]) = {
    // All shuffle read should be local instead of remote
    childrenPlans.foreach {
      case input: ShuffleQueryStageInput =>
        input.isLocalShuffle = true
      case _ =>
    }
    // If there's shuffle write on broadcast side, then find the partitions with 0 size and ignore
    // reading them in local shuffle read.
    broadcastSidePlan match {
      case broadcast: ShuffleQueryStageInput
        if broadcast.childStage.stats.bytesByPartitionId.isDefined =>
        val (startIndicies, endIndicies) = calculatePartitionStartEndIndices(broadcast.childStage
          .stats.bytesByPartitionId.get)
        broadcast.partitionStartIndices = Some(startIndicies)
        broadcast.partitionEndIndices = Some(endIndicies)
        joinType match {
          case _: InnerLike =>
            childrenPlans.foreach {
              case input: ShuffleQueryStageInput =>
                input.partitionStartIndices = Some(startIndicies)
                input.partitionEndIndices = Some(endIndicies)
              case _ =>
            }
          case _ =>
        }
      case _ =>
    }
  }

  // While the changes in optimizeForLocalShuffleReadLessPartitions has additional exchanges,
  // we need to revert this changes.
  private def revertShuffleReadChanges(
      childrenPlans: Seq[SparkPlan]) = {
    childrenPlans.foreach {
      case input: ShuffleQueryStageInput =>
        input.isLocalShuffle = false
        input.partitionEndIndices = None
        input.partitionStartIndices = None
      case _ =>
    }
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
            leftKeys,
            rightKeys,
            joinType,
            buildSide,
            condition,
            removeSort(left),
            removeSort(right))

          val newChild = queryStage.child.transformDown {
            case s: SortMergeJoinExec if (s.fastEquals(smj)) => broadcastJoin
          }

          val broadcastSidePlan = buildSide match {
            case BuildLeft => (removeSort(left))
            case BuildRight => (removeSort(right))
          }
          // Local shuffle read less partitions based on broadcastSide's row statistics
          optimizeForLocalShuffleReadLessPartitions(joinType, broadcastSidePlan,
              broadcastJoin.children)

          // Apply EnsureRequirement rule to check if any new Exchange will be added. If the added
          // Exchange number less than spark.sql.adaptive.maxAdditionalShuffleNum, we convert the
          // sortMergeJoin to BroadcastHashJoin. Otherwise we don't convert it because it causes
          // additional Shuffle.
          val afterEnsureRequirements = EnsureRequirements(conf).apply(newChild)
          val numExchanges = afterEnsureRequirements.collect {
            case e: ShuffleExchange => e
          }.length

          val maxAdditionalShuffleNum = conf.adaptiveMaxAdditionalShuffleNum
          if ((numExchanges == 0) ||
            (queryStage.isInstanceOf[ShuffleQueryStage] &&
              numExchanges <= maxAdditionalShuffleNum + 1)) {
            // Update the plan in queryStage
            queryStage.child = newChild
            broadcastJoin
          } else {
            logWarning("Join optimization not applied due to the number of added exchanges" +
              " introduced is larger than spark.sql.adaptive.maxAdditionalShuffleNum")
            revertShuffleReadChanges(broadcastJoin.children)
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
