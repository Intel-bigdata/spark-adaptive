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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf

/**
 * Ensures that the [[org.apache.spark.sql.catalyst.plans.physical.Partitioning Partitioning]]
 * of input data meets the
 * [[org.apache.spark.sql.catalyst.plans.physical.Distribution Distribution]] requirements for
 * each operator by inserting [[ShuffleExchange]] Operators where required.  Also ensure that the
 * input partition ordering requirements are met.
 */
case class EnsureRequirements(conf: SQLConf) extends Rule[SparkPlan] {
  private def defaultNumPreShufflePartitions: Int =
    if (conf.adaptiveExecutionEnabled) {
      conf.maxNumPostShufflePartitions
    } else {
      conf.numShufflePartitions
    }

  /**
   * Given a required distribution, returns a partitioning that satisfies that distribution.
   */
  private def createPartitioning(
      requiredDistribution: Distribution,
      numPartitions: Int): Partitioning = {
    requiredDistribution match {
      case AllTuples => SinglePartition
      case ClusteredDistribution(clustering) => HashPartitioning(clustering, numPartitions)
      case OrderedDistribution(ordering) => RangePartitioning(ordering, numPartitions)
      case dist => sys.error(s"Do not know how to satisfy distribution $dist")
    }
  }

  private def ensureDistributionAndOrdering(operator: SparkPlan): SparkPlan = {
    val requiredChildDistributions: Seq[Distribution] = operator.requiredChildDistribution
    val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering
    var children: Seq[SparkPlan] = operator.children
    assert(requiredChildDistributions.length == children.length)
    assert(requiredChildOrderings.length == children.length)

    // Ensure that the operator's children satisfy their output distribution requirements:
    children = children.zip(requiredChildDistributions).map {
      case (child, distribution) if child.outputPartitioning.satisfies(distribution) =>
        child
      case (child, BroadcastDistribution(mode)) =>
        BroadcastExchangeExec(mode, child)
      case (child, distribution) =>
        ShuffleExchange(createPartitioning(distribution, defaultNumPreShufflePartitions), child)
    }

    // If the operator has multiple children and specifies child output distributions (e.g. join),
    // then the children's output partitionings must be compatible:
    def requireCompatiblePartitioning(distribution: Distribution): Boolean = distribution match {
      case UnspecifiedDistribution => false
      case BroadcastDistribution(_) => false
      case _ => true
    }
    if (children.length > 1
        && requiredChildDistributions.exists(requireCompatiblePartitioning)
        && !Partitioning.allCompatible(children.map(_.outputPartitioning))) {

      // First check if the existing partitions of the children all match. This means they are
      // partitioned by the same partitioning into the same number of partitions. In that case,
      // don't try to make them match `defaultPartitions`, just use the existing partitioning.
      val maxChildrenNumPartitions = children.map(_.outputPartitioning.numPartitions).max
      val useExistingPartitioning = children.zip(requiredChildDistributions).forall {
        case (child, distribution) =>
          child.outputPartitioning.guarantees(
            createPartitioning(distribution, maxChildrenNumPartitions))
      }

      children = if (useExistingPartitioning) {
        // We do not need to shuffle any child's output.
        children
      } else {
        // We need to shuffle at least one child's output.
        // Now, we will determine the number of partitions that will be used by created
        // partitioning schemes.
        val numPartitions = {
          // Let's see if we need to shuffle all child's outputs when we use
          // maxChildrenNumPartitions.
          val shufflesAllChildren = children.zip(requiredChildDistributions).forall {
            case (child, distribution) =>
              !child.outputPartitioning.guarantees(
                createPartitioning(distribution, maxChildrenNumPartitions))
          }
          // If we need to shuffle all children, we use defaultNumPreShufflePartitions as the
          // number of partitions. Otherwise, we use maxChildrenNumPartitions.
          if (shufflesAllChildren) defaultNumPreShufflePartitions else maxChildrenNumPartitions
        }

        children.zip(requiredChildDistributions).map {
          case (child, distribution) =>
            val targetPartitioning = createPartitioning(distribution, numPartitions)
            if (child.outputPartitioning.guarantees(targetPartitioning)) {
              child
            } else {
              child match {
                // If child is an exchange, we replace it with
                // a new one having targetPartitioning.
                case ShuffleExchange(_, c) => ShuffleExchange(targetPartitioning, c)
                case _ => ShuffleExchange(targetPartitioning, child)
              }
          }
        }
      }
    }

    // Now that we've performed any necessary shuffles, add sorts to guarantee output orderings:
    children = children.zip(requiredChildOrderings).map { case (child, requiredOrdering) =>
      if (requiredOrdering.nonEmpty) {
        // If child.outputOrdering is [a, b] and requiredOrdering is [a], we do not need to sort.
        val orderingMatched = if (requiredOrdering.length > child.outputOrdering.length) {
          false
        } else {
          requiredOrdering.zip(child.outputOrdering).forall {
            case (requiredOrder, childOutputOrder) =>
              childOutputOrder.satisfies(requiredOrder)
          }
        }

        if (!orderingMatched) {
          SortExec(requiredOrdering, global = false, child = child)
        } else {
          child
        }
      } else {
        child
      }
    }

    operator.withNewChildren(children)
  }

  def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case operator @ ShuffleExchange(partitioning, child) =>
      child.children match {
        case ShuffleExchange(childPartitioning, baseChild)::Nil =>
          if (childPartitioning.guarantees(partitioning)) child else operator
        case _ => operator
      }
    case operator: SparkPlan => ensureDistributionAndOrdering(operator)
  }
}
