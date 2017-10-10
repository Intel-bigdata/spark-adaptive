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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql._
import org.apache.spark.sql.execution.adaptive.{OptimizeJoin, QueryStageInput, ShuffleQueryStageInput}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

class QueryStageSuite extends SparkFunSuite with BeforeAndAfterAll {

  private var originalActiveSparkSession: Option[SparkSession] = _
  private var originalInstantiatedSparkSession: Option[SparkSession] = _

  override protected def beforeAll(): Unit = {
    originalActiveSparkSession = SparkSession.getActiveSession
    originalInstantiatedSparkSession = SparkSession.getDefaultSession

    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  override protected def afterAll(): Unit = {
    // Set these states back.
    originalActiveSparkSession.foreach(ctx => SparkSession.setActiveSession(ctx))
    originalInstantiatedSparkSession.foreach(ctx => SparkSession.setDefaultSession(ctx))
  }

  def defaultSparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.allowMultipleContexts", "true")
      .config(SQLConf.SHUFFLE_PARTITIONS.key, "5")
      .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .config(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      .config(SQLConf.ADAPTIVE_BROADCASTJOIN_THRESHOLD.key, "12000")
      .getOrCreate()
    spark
  }

  def withSparkSession(spark: SparkSession)(f: SparkSession => Unit): Unit = {
    try f(spark) finally spark.stop()
  }

  val numInputPartitions: Int = 10

  def checkAnswer(actual: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    QueryTest.checkAnswer(actual, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  test("1 sort merge join to broadcast join") {
    withSparkSession(defaultSparkSession) { spark: SparkSession =>
      val df1 =
        spark
          .range(0, 1000, 1, numInputPartitions)
          .selectExpr("id % 500 as key1", "id as value1")
      val df2 =
        spark
          .range(0, 1000, 1, numInputPartitions)
          .selectExpr("id % 500 as key2", "id as value2")

      val join = df1.join(df2, col("key1") === col("key2")).select(col("key1"), col("value2"))

      // Before Execution, there is one SortMergeJoin
      val smjBeforeExecution = join.queryExecution.executedPlan.collect {
        case smj: SortMergeJoinExec => smj
      }
      assert(smjBeforeExecution.length === 1)

      // Check the answer.
      val expectedAnswer =
        spark
          .range(0, 1000)
          .selectExpr("id % 500 as key", "id as value")
          .union(spark.range(0, 1000).selectExpr("id % 500 as key", "id as value"))
      checkAnswer(
        join,
        expectedAnswer.collect())

      // During execution, the SortMergeJoin is changed to BroadcastHashJoinExec
      val smjAfterExecution = join.queryExecution.executedPlan.collect {
        case smj: SortMergeJoinExec => smj
      }
      assert(smjAfterExecution.length === 0)

      val numBhjAfterExecution = join.queryExecution.executedPlan.collect {
        case smj: BroadcastHashJoinExec => smj
      }.length
      assert(numBhjAfterExecution === 1)

      val queryStageInputs = join.queryExecution.executedPlan.collect {
        case q: QueryStageInput => q
      }
      assert(queryStageInputs.length === 2)
    }
  }

  test("2 sort merge joins to broadcast joins") {
    // t1 and t3 are smaller than the spark.sql.adaptiveBroadcastJoinThreshold
    // t2 is greater than spark.sql.adaptiveBroadcastJoinThreshold
    // Both Join1 and Join2 are changed to broadcast join.
    //
    //              Join2
    //              /   \
    //          Join1   Ex (Exchange)
    //          /   \    \
    //        Ex    Ex   t3
    //       /       \
    //      t1       t2
    withSparkSession(defaultSparkSession) { spark: SparkSession =>
      val df1 =
        spark
          .range(0, 1000, 1, numInputPartitions)
          .selectExpr("id % 500 as key1", "id as value1")
      val df2 =
        spark
          .range(0, 1000, 1, numInputPartitions)
          .selectExpr("id % 500 as key2", "id as value2")
      val df3 =
        spark
          .range(0, 500, 1, numInputPartitions)
          .selectExpr("id % 500 as key3", "id as value3")

      val join =
        df1
        .join(df2, col("key1") === col("key2"))
        .join(df3, col("key2") === col("key3"))
        .select(col("key3"), col("value1"))

      // Before Execution, there is two SortMergeJoins
      val smjBeforeExecution = join.queryExecution.executedPlan.collect {
        case smj: SortMergeJoinExec => smj
      }
      assert(smjBeforeExecution.length === 2)

      // Check the answer.
      val expectedAnswer =
        spark
          .range(0, 1000)
          .selectExpr("id % 500 as key", "id as value")
          .union(spark.range(0, 1000).selectExpr("id % 500 as key", "id as value"))
      checkAnswer(
        join,
        expectedAnswer.collect())

      // During execution, 2 SortMergeJoin are changed to BroadcastHashJoin
      val smjAfterExecution = join.queryExecution.executedPlan.collect {
        case smj: SortMergeJoinExec => smj
      }
      assert(smjAfterExecution.length === 0)

      val numBhjAfterExecution = join.queryExecution.executedPlan.collect {
        case smj: BroadcastHashJoinExec => smj
      }.length
      assert(numBhjAfterExecution === 2)

      val queryStageInputs = join.queryExecution.executedPlan.collect {
        case q: QueryStageInput => q
      }
      assert(queryStageInputs.length === 3)
    }
  }

  test("Do not change sort merge join if it adds additional Exchanges") {
    // t1 is smaller than spark.sql.adaptiveBroadcastJoinThreshold
    // t2 and t3 are greater than spark.sql.adaptiveBroadcastJoinThreshold
    // Both Join1 and Join2 are not changed to broadcast join.
    //
    //              Join2
    //              /   \
    //          Join1   Ex (Exchange)
    //          /   \    \
    //        Ex    Ex   t3
    //       /       \
    //      t1       t2
    withSparkSession(defaultSparkSession) { spark: SparkSession =>
      val df1 =
        spark
          .range(0, 1000, 1, numInputPartitions)
          .selectExpr("id % 500 as key1", "id as value1")
      val df2 =
        spark
          .range(0, 1000, 1, numInputPartitions)
          .selectExpr("id % 500 as key2", "id as value2")
      val df3 =
        spark
          .range(0, 1500, 1, numInputPartitions)
          .selectExpr("id % 500 as key3", "id as value3")

      val join =
        df1
        .join(df2, col("key1") === col("key2"))
        .join(df3, col("key2") === col("key3"))
        .select(col("key3"), col("value1"))

      // Before Execution, there is two SortMergeJoins
      val smjBeforeExecution = join.queryExecution.executedPlan.collect {
        case smj: SortMergeJoinExec => smj
      }
      assert(smjBeforeExecution.length === 2)

      // Check the answer.
      val partResult =
        spark
          .range(0, 1000)
          .selectExpr("id % 500 as key", "id as value")
          .union(spark.range(0, 1000).selectExpr("id % 500 as key", "id as value"))
      val expectedAnswer = partResult.union(partResult).union(partResult)
      checkAnswer(
        join,
        expectedAnswer.collect())

      // During execution, no SortMergeJoin is changed to BroadcastHashJoin
      val smjAfterExecution = join.queryExecution.executedPlan.collect {
        case smj: SortMergeJoinExec => smj
      }
      assert(smjAfterExecution.length === 2)

      val numBhjAfterExecution = join.queryExecution.executedPlan.collect {
        case smj: BroadcastHashJoinExec => smj
      }.length
      assert(numBhjAfterExecution === 0)

      val queryStageInputs = join.queryExecution.executedPlan.collect {
        case q: QueryStageInput => q
      }
      assert(queryStageInputs.length === 3)
    }
  }

  test("Reuse QueryStage in adaptive execution") {
    withSparkSession(defaultSparkSession) { spark: SparkSession =>
      val df = spark.range(0, 1000, 1, numInputPartitions).toDF()
      val join = df.join(df, "id")

      // Before Execution, there is one SortMergeJoin
      val smjBeforeExecution = join.queryExecution.executedPlan.collect {
        case smj: SortMergeJoinExec => smj
      }
      assert(smjBeforeExecution.length === 1)

      checkAnswer(join, df.collect())

      // During execution, the SortMergeJoin is changed to BroadcastHashJoinExec
      val smjAfterExecution = join.queryExecution.executedPlan.collect {
        case smj: SortMergeJoinExec => smj
      }
      assert(smjAfterExecution.length === 0)

      val numBhjAfterExecution = join.queryExecution.executedPlan.collect {
        case smj: BroadcastHashJoinExec => smj
      }.length
      assert(numBhjAfterExecution === 1)

      val queryStageInputs = join.queryExecution.executedPlan.collect {
        case q: QueryStageInput => q
      }
      assert(queryStageInputs.length === 2)

      assert(queryStageInputs(0).childStage === queryStageInputs(1).childStage)
    }
  }

  test("adaptive skewed join") {
    val spark = defaultSparkSession
    spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_JOIN_ENABLED.key, "false")
    spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_SKEWED_JOIN_ENABLED.key, "true")
    spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_SKEWED_PARTITION_ROW_COUNT_THRESHOLD.key, 10)
    withSparkSession(spark) { spark: SparkSession =>
      val df1 =
        spark
          .range(0, 10, 1, 2)
          .selectExpr("id % 5 as key1", "id as value1")
      val df2 =
        spark
          .range(0, 1000, 1, numInputPartitions)
          .selectExpr("id % 1 as key2", "id as value2")

      val join = df1.join(df2, col("key1") === col("key2")).select(col("key1"), col("value2"))

      // Before Execution, there is one SortMergeJoin
      val smjBeforeExecution = join.queryExecution.executedPlan.collect {
        case smj: SortMergeJoinExec => smj
      }
      assert(smjBeforeExecution.length === 1)

      // Check the answer.
      val expectedAnswer =
        spark
          .range(0, 1000)
          .selectExpr("0 as key", "id as value")
          .union(spark.range(0, 1000).selectExpr("0 as key", "id as value"))
      checkAnswer(
        join,
        expectedAnswer.collect())

      // During execution, the SMJ is changed to Union of SMJ + 5 SMJ of the skewed partition.
      val smjAfterExecution = join.queryExecution.executedPlan.collect {
        case smj: SortMergeJoinExec => smj
      }
      assert(smjAfterExecution.length === 6)

      val queryStageInputs = join.queryExecution.executedPlan.collect {
        case q: ShuffleQueryStageInput => q
      }
      assert(queryStageInputs.length === 2)
      assert(queryStageInputs(0).skewedPartitions === queryStageInputs(1).skewedPartitions)
      assert(queryStageInputs(0).skewedPartitions === Some(Set(0)))
    }
  }

  test("Calculate local shuffle read partition ranges") {
    val testArrays = Array(
      Array(0L, 0, 1, 2, 0, 1, 2, 0),
      Array(1L, 1, 0),
      Array(0L, 1, 0),
      Array(0L, 0),
      Array(1L, 2, 3),
      Array[Long]()
    )
    val anserStart = Array(
      Array(2, 5),
      Array(0),
      Array(1),
      Array(0),
      Array(0),
      Array(0)
    )
    val anserEnd = Array(
      Array(4, 7),
      Array(2),
      Array(2),
      Array(0),
      Array(3),
      Array(0)
    )
    val func = OptimizeJoin(new SQLConf).calculatePartitionStartEndIndices _
    testArrays.zip(anserStart).zip(anserEnd).foreach {
      case ((parameter, expectStart), expectEnd) =>
        val (resultStart, resultEnd) = func(parameter)
        assert(resultStart.deep == expectStart.deep)
        assert(resultEnd.deep == expectEnd.deep)
      case _ =>
    }
  }
}
