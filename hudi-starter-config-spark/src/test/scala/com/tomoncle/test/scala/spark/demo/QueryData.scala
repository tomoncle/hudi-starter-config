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

import com.tomoncle.test.scala.spark.SparkHudiUtils._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.junit.Test
import org.slf4j.{Logger, LoggerFactory}

/**
  * 当Hudi中表的类型为：COW时，支持2种方式查询：Snapshot Queries、Incremental Queries；
  * 默认情况下查询属于：Snapshot Queries快照查询，通过参数：hoodie.datasource.query.type 可以进行设置
  */
class QueryData {

  protected lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private lazy val TABLE_NAME: String = "hudi_trips_query_cow"
  private lazy val BASE_PATH: String = "s3a://test-apache-hudi/" + TABLE_NAME

  def initData(spark: SparkSession, tableName: String, basePath: String): Unit = {
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.QuickstartUtils._
    import org.apache.hudi.config.HoodieWriteConfig._
    import spark.implicits._

    import scala.collection.JavaConverters._

    val dataGen: DataGenerator = new DataGenerator()
    val inserts = convertToStringList(dataGen.generateInserts(100))
    val insertDF = spark.read.json(spark.sparkContext.parallelize(inserts.asScala, 2).toDS())
    insertDF.write.mode(SaveMode.Append).format("hudi")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option(PRECOMBINE_FIELD.key(), "ts")
      .option(RECORDKEY_FIELD.key(), "uuid")
      .option(PARTITIONPATH_FIELD.key(), "partitionpath")
      .option(TBL_NAME.key(), tableName)
      .save(basePath)
  }

  /**
    * 快照方式查询（Snapshot Query）数据，采用DSL方式
    */
  def queryDataBySnapshot(spark: SparkSession, path: String): Unit = {
    import spark.implicits._
    logger.info("初始化spark的s3配置：")
    val queryDF: DataFrame = spark.read.format("hudi").load(path)
    logger.info("打印表结构：")
    queryDF.printSchema()
    logger.info("输出前10条数据：")
    queryDF.show(10, truncate = false)
    logger.info("查询费用大于20，小于50的乘车数据：")
    queryDF
      .filter($"fare" >= 20 && $"fare" <= 50)
      .select($"driver", $"rider", $"fare", $"begin_lat", $"begin_lon", $"partitionpath", $"_hoodie_commit_time")
      .orderBy($"fare".desc, $"_hoodie_commit_time".desc)
      .show(20, truncate = false)
  }

  /**
    * 快照方式查询（Snapshot Query）数据，采用SQL方式
    *
    * @param spark SparkSession
    * @param path  保存路径
    */
  def queryDataBySnapshotSQL(spark: SparkSession, path: String): Unit ={
    val tripsSnapshotDF = spark.read.format("hudi").load(path)
    println(tripsSnapshotDF.show(10,truncate = false))
    tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")
    logger.info("查询费用大于20的数据：")
    spark.sql("select fare, begin_lon, begin_lat, ts from hudi_trips_snapshot where fare > 20.0").show()
    logger.info("查询所有数据：")
    spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from hudi_trips_snapshot").show()
    logger.info("分组查询：")
    spark.sql("select driver, count(1), sum(fare) from hudi_trips_snapshot group by driver order by driver asc").show()
  }

  /**
    * 快照方式查询（Snapshot Query）数据，采用DSL方式
    * 可以依据时间进行过滤查询，设置属性："as.of.instant"
    *
    * @param spark SparkSession
    * @param path  保存路径
    */
  def queryDataByTime(spark: SparkSession, path: String): Unit = {
    import org.apache.spark.sql.functions._

    logger.info("方式一：指定字符串，按照日期时间过滤获取数据：")
    val time = "20220329055842"
    val df1 = spark.read.format("hudi").option("as.of.instant", time).load(path)
      .sort(col("_hoodie_commit_time").desc)
    df1.printSchema()
    df1.show(numRows = 5, truncate = false)

    logger.info("方式二：指定字符串，按照日期时间过滤获取数据：")
    val timeStr = "2022-03-29 05:58:42.000"
    val df2 = spark.read.format("hudi").option("as.of.instant", timeStr).load(path)
      .sort(col("_hoodie_commit_time").desc)
    df2.printSchema()
    df2.show(numRows = 5, truncate = false)
  }

  @Test
  def appInit(): Unit = {
    logger.debug("加载 HADOOP_HOME 环境变量：")
    val spark = getS3SparkInstance
    initData(spark, TABLE_NAME, BASE_PATH)
  }

  @Test
  def appRunQueryDataByTime(): Unit = {
    logger.debug("加载 HADOOP_HOME 环境变量：")
    val spark = getS3SparkInstance
    queryDataByTime(spark, BASE_PATH)
  }

  @Test
  def appRunQueryDataBySnapshot(): Unit = {
    logger.debug("加载 HADOOP_HOME 环境变量：")
    val spark = getS3SparkInstance
    queryDataBySnapshot(spark, BASE_PATH)
  }

  @Test
  def appRunQueryDataBySnapshotSQL(): Unit = {
    logger.debug("加载 HADOOP_HOME 环境变量：")
    val spark = getS3SparkInstance
    queryDataBySnapshotSQL(spark, BASE_PATH)
  }
}
