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

import org.apache.spark.sql.catalyst.expressions.RowOrdering
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

// optimize the sort merge join to shuffle hash join
case class OptimizeSMJtoSHJ(conf: SQLConf) extends Rule[SparkPlan] {
  /**
   * Returns whether plan a is much smaller (3X) than plan b.
   *
   * The cost to build hash map is higher than sorting, we should only build hash map on a table
   * that is much smaller than other one. Since we does not have the statistic for number of rows,
   * use the size of bytes here as estimation.
   */
  private def muchSmaller(a: SparkPlan, b: SparkPlan): Boolean = {
    a.stats.sizeInBytes * conf.adaptiveHashJoinFactor <= b.stats.sizeInBytes
  }

  /**
   * Matches a plan whose single partition should be small enough to build a hash table.
   *
   * Note: this assume that the number of partition is fixed, requires additional work if it's
   * dynamic.
   */
  private def canBuildLocalHashMap(plan: SparkPlan, initialPlan: SparkPlan): Boolean = {
    plan.stats.sizeInBytes < conf.adaptiveBroadcastJoinThreshold *
      (EnsureRequirements(conf).defaultNumPreShufflePartitions(initialPlan))
  }

  private def canBuildRight(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | LeftOuter | LeftSemi | LeftAnti => true
    case j: ExistenceJoin => true
    case _ => false
  }

  private def canBuildLeft(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | RightOuter => true
    case _ => false
  }

  private def removeSort(plan: SparkPlan): SparkPlan = {
    plan match {
      case s: SortExec => s.child
      case p: SparkPlan => p
    }
  }

  private def optimizeSortMergeJoin(
      smj: SortMergeJoinExec,
      queryStage: QueryStage,
      initialPlan: SparkPlan): SparkPlan = {
    smj match {
      case SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right) =>
        val hashJoinBuildSide = if (canBuildRight(joinType)
          && (muchSmaller(removeSort(right), removeSort(left))
          && canBuildLocalHashMap(removeSort(right), initialPlan))
          || !RowOrdering.isOrderable(leftKeys)) {
          Some(BuildRight)
        } else if (canBuildLeft(joinType)
          && (muchSmaller(removeSort(left), removeSort(right))
          && canBuildLocalHashMap(removeSort(left), initialPlan))
          || !RowOrdering.isOrderable(rightKeys)) {
          Some(BuildLeft)
        } else {
          None
        }

        hashJoinBuildSide.map { buildSide =>
          val hashJoin = ShuffledHashJoinExec(
            leftKeys,
            rightKeys,
            joinType,
            buildSide,
            condition,
            removeSort(left),
            removeSort(right))
          val newChild = queryStage.child.transformDown {
            case s: SortMergeJoinExec if s.fastEquals(smj) => hashJoin
          }

          // Apply EnsureRequirement rule to check if any new Exchange will be added.
          // If new exchange added, disable to convert the smj to shj.
          val afterEnsureRequirements = EnsureRequirements(conf).apply(newChild)
          val numExchanges = afterEnsureRequirements.collect {
            case e: ShuffleExchangeExec => e
          }.length
          val topShuffleCheck = queryStage match {
            case _: ShuffleQueryStage => afterEnsureRequirements.isInstanceOf[ShuffleExchangeExec]
            case _ => true
          }
          val noAdditionalShuffle = (numExchanges == 0) ||
            (queryStage.isInstanceOf[ShuffleQueryStage] && numExchanges <= 1)

          if (topShuffleCheck && noAdditionalShuffle) {
            queryStage.child = newChild
            hashJoin
          } else {
            logWarning("Join optimization is not applied due to additional shuffles will be " +
              "introduced.")
            smj
          }
        }.getOrElse(smj)
    }
  }

  private def optimizeJoin(
      operator: SparkPlan,
      queryStage: QueryStage,
      initialPlan: SparkPlan): SparkPlan = {
    operator match {
      case smj: SortMergeJoinExec =>
        val op = optimizeSortMergeJoin(smj, queryStage, initialPlan)
        val optimizedChildren = op.children.map(optimizeJoin(_, queryStage, initialPlan))
        op.withNewChildren(optimizedChildren)
      case op =>
        val optimizedChildren = op.children.map(optimizeJoin(_, queryStage, initialPlan))
        op.withNewChildren(optimizedChildren)
    }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.adaptiveHashJoinEnabled) {
      plan
    } else {
      plan match {
        case queryStage: QueryStage =>
          val optimizedPlan = optimizeJoin(queryStage.child, queryStage, plan)
          queryStage.child = optimizedPlan
          queryStage
        case _ => plan
      }
    }
  }
}
