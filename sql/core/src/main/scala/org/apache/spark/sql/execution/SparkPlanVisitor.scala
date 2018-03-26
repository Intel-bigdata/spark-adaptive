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

import org.apache.spark.sql.execution.adaptive.QueryStage
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchange
import org.apache.spark.sql.execution.joins.{HashJoin, SortMergeJoinExec}

/**
 * A visitor pattern for traversing a [[SparkPlan]] tree and compute some properties.
 */
trait SparkPlanVisitor[T] {

  def visit(p: SparkPlan): T = p match {
    case p: FilterExec => visitFilterExec(p)
    case p: HashAggregateExec => visitHashAggregateExec(p)
    case p: HashJoin => visitHashJoin(p)
    case p: ProjectExec => visitProjectExec(p)
    case p: ShuffleExchange => visitShuffleExchange(p)
    case p: SortAggregateExec => visitSortAggregateExec(p)
    case p: SortMergeJoinExec => visitSortMergeJoinExec(p)
    case p: QueryStage => visitQueryStage(p)
    case p: SparkPlan => default(p)
  }

  def default(p: SparkPlan): T

  def visitFilterExec(p: FilterExec): T

  def visitHashAggregateExec(p: HashAggregateExec): T

  def visitHashJoin(p: HashJoin): T

  def visitProjectExec(p: ProjectExec): T

  def visitShuffleExchange(p: ShuffleExchange): T

  def visitSortAggregateExec(p: SortAggregateExec): T

  def visitSortMergeJoinExec(p: SortMergeJoinExec): T

  def visitQueryStage(p: QueryStage): T
}
