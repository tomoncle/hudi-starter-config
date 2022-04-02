/*
 * Copyright 2018 tomoncle
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tomoncle.test.scala.spark.demo

import com.tomoncle.test.scala.spark.SparkHudiUtils.getS3SparkInstance
import org.apache.spark.sql.SparkSession
import org.junit.Test

/**
  * 增量查询（Incremental Query）数据
  *
  * incremental增量查询，需要指定时间戳，当Hudi表中数据满足：instant_time > beginTime时，数据将会被加载读取。
  * 此外，可设置某个时间范围：endTime > instant_time > beginTime ，获取相应的数据
  */
class IncrementalQueryData {
  @Test
  def app(): Unit = {
    val spark = getS3SparkInstance
    incrementalQueryData(spark, "s3a://test-apache-hudi/hudi_trips_query_cow")
  }

  def incrementalQueryData(spark: SparkSession, path: String): Unit = {
    import spark.implicits._

    // 第1步、加载Hudi表数据，获取commit time时间，作为增量查询数据阈值
    import org.apache.hudi.DataSourceReadOptions._
    spark.read
      .format("hudi")
      .load(path)
      .createOrReplaceTempView("view_temp_hudi_trips")
    val commits: Array[String] = spark.sql(
      """
        				  |select
        				  |  distinct(_hoodie_commit_time) as commitTime
        				  |from
        				  |  view_temp_hudi_trips
        				  |order by
        				  |  commitTime DESC
        				  |""".stripMargin)
      .map(row => row.getString(0))
      .take(50)
    val beginTime = commits(commits.length - 1) // commit time we are interested in
    println(s"beginTime = ${beginTime}")

    // 第2步、设置Hudi数据CommitTime时间阈值，进行增量数据查询
    val tripsIncrementalDF = spark.read
      .format("hudi")
      // 设置查询数据模式为：incremental，增量读取
      .option(QUERY_TYPE.key(), QUERY_TYPE_INCREMENTAL_OPT_VAL)
      // 设置增量读取数据时开始时间
      .option(BEGIN_INSTANTTIME.key(), beginTime)
      .load(path)

    // 第3步、将增量查询数据注册为临时视图，查询费用大于20数据
    tripsIncrementalDF.createOrReplaceTempView("hudi_trips_incremental")
    spark.sql(
      """
        				  |select
        				  |  `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts
        				  |from
        				  |  hudi_trips_incremental
        				  |where
        				  |  fare > 20.0
        				  |""".stripMargin)
      .show(10, truncate = false)
  }
}
