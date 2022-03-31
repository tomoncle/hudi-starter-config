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

package com.tomoncle.test.scala.spark

import com.tomoncle.test.scala.spark.SparkHudiUtils.getS3SparkInstance
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.junit.Test
import org.slf4j.{Logger, LoggerFactory}

/**
df.write.format("hudi")
      .option(TABLE_NAME, "hudi_test_0")
      .option(OPERATION_OPT_KEY, DELETE_OPERATION_OPT_VAL) // for delete
      .option(RECORDKEY_FIELD_OPT_KEY, "id")
      .option(PRECOMBINE_FIELD_OPT_KEY, "version")
      .option(KEYGENERATOR_CLASS_OPT_KEY, classOf[SimpleKeyGenerator].getName)
      .option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getCanonicalName)
      .option(PARTITIONPATH_FIELD_OPT_KEY, "dt")
      .option(DELETE_PARALLELISM, "8")
      .mode(Append)
      .save("/tmp/hudi/h0")
  */
class DeleteData {
  protected lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private lazy val TABLE_NAME: String = "hudi_trips_query_cow"
  private lazy val BASE_PATH: String = "s3a://test-apache-hudi/" + TABLE_NAME

  /**
    * 删除Hudi表数据，依据主键UUID进行删除，如果是分区表，指定分区路径
    */
  def deleteData(spark: SparkSession, tableName: String, basePath: String): Unit = {
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.QuickstartUtils._
    import org.apache.hudi.config.HoodieWriteConfig._
    import spark.implicits._

    import scala.collection.JavaConverters._

    // 第1步、加载Hudi表数据，获取条目数
    val queryDF: DataFrame = spark.read.format("hudi").load(basePath)
    logger.info(s"Raw Count = ${queryDF.count()}")

    // 第2步、模拟要删除的数据，从Hudi中加载数据，获取几条数据，转换为要删除数据集合
    val dataFrame = queryDF.limit(2).select($"uuid", $"partitionpath")
    val dataGenerator = new DataGenerator()
    val deletes = dataGenerator.generateDeletes(dataFrame.collectAsList())
    val deleteDF = spark.read.json(spark.sparkContext.parallelize(deletes.asScala, 2).toDS())

    // 第3步、保存数据到Hudi表中，设置操作类型：DELETE
    deleteDF.write.mode(SaveMode.Append).format("hudi")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option("hoodie.delete.shuffle.parallelism", "2")
      .option(OPERATION.key(), DELETE_OPERATION_OPT_VAL) // 设置数据操作类型为delete，默认值为upsert
      .option(PRECOMBINE_FIELD.key(), "ts")
      .option(RECORDKEY_FIELD.key(), "uuid")
      .option(PARTITIONPATH_FIELD.key(), "partitionpath")
      .option(TBL_NAME.key(), tableName)
      .save(basePath)

    // 第4步、再次加载Hudi表数据，统计条目数，查看是否减少2条数据
    val queryDF2: DataFrame = spark.read.format("hudi").load(basePath)
    logger.info(s"Delete After Count = ${queryDF2.count()}")
  }

  @Test
  def app(): Unit = {
    val spark = getS3SparkInstance
    deleteData(spark, TABLE_NAME, BASE_PATH)
  }
}
