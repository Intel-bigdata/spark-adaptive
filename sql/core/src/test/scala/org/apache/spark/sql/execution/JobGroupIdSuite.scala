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

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import scala.language.existentials

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.status.JobDataWrapper

class JobGroupIdSuite extends QueryTest with AEContext {

  test("adaptive execution should run with right Job Group") {
    val isFinished = new AtomicInteger(0)
    class Worker(id: String, desc: String) extends Runnable {
      override def run(): Unit = {
        spark.sparkContext.setJobGroup(id, desc)
        spark.sql("select 1 as key, 'a' as v1").createOrReplaceTempView("test")
        spark.sql("select 1 as key, '2' as v2").createOrReplaceTempView("test2")
        spark.sql("select * from test join test2 on test.key = test2.key").show
        isFinished.incrementAndGet()
      }
    }
    val executor = Executors.newFixedThreadPool(1)
    val w1 = new Worker("1", "job group one")
    executor.submit(w1)
    val w2 = new Worker("2", "job group tow")
    executor.submit(w2)

    // waiting for job finish.
    while (isFinished.get() < 2) {
      Thread.sleep(1000)
    }
    val jobStart = spark.sparkContext.statusStore.store.read(classOf[JobDataWrapper], 2)
    assert(jobStart.info.jobGroup == Some("2"))
  }
}

trait AEContext extends SharedSQLContext {
  override def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.join.enabled", "true")
    conf.set("spark.sql.adaptive.skewedJoin.enabled", "true")
    conf.set("spark.ui.enabled", "true")
    conf.set("spark.sql.crossJoin.enabled", "true")
    conf
  }

}

