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
import org.apache.hudi.QuickstartUtils.DataGenerator
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.junit.Test
import org.slf4j.{Logger, LoggerFactory}

class UpsertData {
  protected lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private lazy val TABLE_NAME: String = "hudi_trips_upsert_cow"
  private lazy val BASE_PATH: String = "s3a://test-apache-hudi/" + TABLE_NAME

  def query(spark: SparkSession, path: String): Unit = {
    val tripsSnapshotDF = spark.read.format("hudi").load(path)
    tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")
    logger.info("查询所有数据：")
    spark.sql("select * from hudi_trips_snapshot").show()
    logger.info("分组查询：")
    spark.sql("select driver, count(1), sum(fare) from hudi_trips_snapshot group by driver order by driver asc").show()
  }

  /**
    * 官方案例：dataGen.generateInserts模拟产生数据，插入Hudi表，表的类型COW
    */
  def insertDataToS3(spark: SparkSession, tableName: String, basePath: String, dataGen: DataGenerator): Unit = {
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.QuickstartUtils._
    import org.apache.hudi.config.HoodieWriteConfig._
    import spark.implicits._

    import scala.collection.JavaConverters._

    // 这里使用 dataGen.generateInserts
    val inserts = convertToStringList(dataGen.generateInserts(100))
    val insertDF = spark.read.json(spark.sparkContext.parallelize(inserts.asScala, 2).toDS())
    // 保存模式使用 Overwrite
    insertDF.write.mode(SaveMode.Overwrite).format("hudi")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option(PRECOMBINE_FIELD.key(), "ts")
      .option(RECORDKEY_FIELD.key(), "uuid")
      .option(PARTITIONPATH_FIELD.key(), "partitionpath")
      .option(TBL_NAME.key(), tableName)
      .save(basePath)
  }

  /**
    * 官方案例：dataGen.generateUpdates模拟产生数据，插入Hudi表，表的类型COW
    */
  def upsertDataToS3(spark: SparkSession, tableName: String, basePath: String, dataGen: DataGenerator): Unit = {
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.QuickstartUtils._
    import org.apache.hudi.config.HoodieWriteConfig._
    import spark.implicits._

    import scala.collection.JavaConverters._

    // 这里使用 dataGen.generateUpdates
    val updates = convertToStringList(dataGen.generateUpdates(100))
    val updateDF: DataFrame = spark.read.json(spark.sparkContext.parallelize(updates.asScala, 2).toDS())
    // 保存模式使用 Append
    updateDF.write.mode(SaveMode.Append).format("hudi")
      .option("hoodie.insert.shuffle.parallelism", "100")
      .option("hoodie.upsert.shuffle.parallelism", "100")
      .option(PRECOMBINE_FIELD.key(), "ts")
      .option(RECORDKEY_FIELD.key(), "uuid")
      .option(PARTITIONPATH_FIELD.key(), "partitionpath")
      .option(TBL_NAME.key(), tableName)
      .save(basePath)
  }

  @Test
  def appRun(): Unit = {
    val spark = getS3SparkInstance
    // 官方提供工具类DataGenerator模拟生成更新update数据时，
    // 必须要与模拟生成插入insert数据使用同一个DataGenerator对象
    val dataGen: DataGenerator = new DataGenerator()
    logger.debug("insertDataToS3：")
    insertDataToS3(spark, TABLE_NAME, BASE_PATH, dataGen)
    query(spark, BASE_PATH)
    logger.debug("upsertDataToS3：")
    upsertDataToS3(spark, TABLE_NAME, BASE_PATH, dataGen)
    query(spark, BASE_PATH)

  }

}
