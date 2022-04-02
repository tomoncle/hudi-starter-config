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
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.junit.Test
import org.slf4j.{Logger, LoggerFactory}

/**
  *
  * RECORDKEY_FIELD_OPT_KEY：每条记录的唯一id，支持多个字段；
  * *
  * PRECOMBINE_FIELD_OPT_KEY：在数据合并的时候使用到，
  * 当 RECORDKEY_FIELD_OPT_KEY 相同时，默认取 PRECOMBINE_FIELD_OPT_KEY 属性配置的字段最大值所对应的行；
  * *
  * PARTITIONPATH_FIELD_OPT_KEY：用于存放数据的分区字段。
  * *
  * hudi更新数据和插入数据很相似（写法几乎一样），更新数据时，
  * 会根据：RECORDKEY_FIELD_OPT_KEY、PRECOMBINE_FIELD_OPT_KEY、PARTITIONPATH_FIELD_OPT_KEY 三个字段对数据进行合并。
  *
  */
class SaveData {
  protected lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private lazy val TABLE_NAME: String = "hudi_trips_save_cow"
  private lazy val BASE_PATH: String = "s3a://test-apache-hudi/" + TABLE_NAME

  @Test
  def helloWorld(): Unit = {
    logger.info(sayHello())
  }

  /**
    * 官方案例：模拟产生数据，插入Hudi表，表的类型COW
    */
  def saveDataToS3(spark: SparkSession, tableName: String, basePath: String): Unit = {
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.QuickstartUtils._
    import org.apache.hudi.config.HoodieWriteConfig._
    import spark.implicits._

    import scala.collection.JavaConverters._

    val dataGen: DataGenerator = new DataGenerator()
    // inserts: java.util.List[String] = [{"ts": 1648296239622, "uuid": "b7585dd3-4455-4f36-a7f8-a8c5b20ba7c8", "rider": "rider-284", "driver": "driver-284", "begin_lat": 0.7340133901254792, "begin_lon": 0.5142184937933181, "end_lat": 0.7814655558162802, "end_lon": 0.6592596683641996, "fare": 49.527694252432056, "partitionpath": "asia/india/chennai"}]
    val inserts = convertToStringList(dataGen.generateInserts(100))
    val insertDF = spark.read.json(spark.sparkContext.parallelize(inserts.asScala, 2).toDS())
    insertDF.write.mode(SaveMode.Append).format("hudi")
      .option("hoodie.insert.shuffle.parallelism", "100")
      .option("hoodie.upsert.shuffle.parallelism", "100")
      .option(PRECOMBINE_FIELD.key(), "ts")
      .option(RECORDKEY_FIELD.key(), "uuid")
      .option(PARTITIONPATH_FIELD.key(), "partitionpath")
      .option(TBL_NAME.key(), tableName)
      .save(basePath)
  }

  def saveDataToLocal(spark: SparkSession): Unit = {
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.config.HoodieWriteConfig._
    import spark.implicits._
    // 构建元组数据
    // ((0,a0,30.0,10000,p0), (1,a1,30.2,10100,p1), (2,a2,30.4,10200,p2), (3,a3,30.6,10300,p3), (4,a4,30.8,10400,p4),
    // (5,a5,31.0,10500,p0), (6,a6,31.2,10600,p1), (7,a7,31.4,10700,p2), (8,a8,31.6,10800,p3), (9,a9,31.8,10900,p4))
    val df = (for (i <- 0 until 10) yield (i, s"a$i", 30 + i * 0.2, 100 * i + 10000, s"p${i % 5}"))
      .toDF("uuid", "name", "price", "ts", "partitionpath")
    println("数据格式：")
    println(df.printSchema())
    println("数据：")
    println(df.show(10, truncate = false))
    val table = "test_cow"
    val path = getConfigValue("hudi.storage.local.dir") + "/00/" + table
    df.write.mode(SaveMode.Append).format("hudi")
      .option("hoodie.insert.shuffle.parallelism", "100")
      .option("hoodie.upsert.shuffle.parallelism", "100")
      .option(PRECOMBINE_FIELD.key(), "ts")
      .option(RECORDKEY_FIELD.key(), "uuid")
      .option(PARTITIONPATH_FIELD.key(), "partitionpath")
      .option(TBL_NAME.key(), table)
      .save(path)
  }

  def saveJsonDataToLocal(spark: SparkSession): Unit = {
    import org.apache.hudi.config.HoodieWriteConfig._
    // 加载json数据
    val filePath = getConfigValue("test.json.path")
    val dataset: Dataset[String] = loadJsonFileToDataset(filePath, spark)
    val df = spark.read.json(dataset)
    //    println(df.printSchema())
    //    println(df.show(10, truncate = false))
    val table = "on_time_cow"
    val path = getConfigValue("hudi.storage.local.dir") + "/01/" + table
    df.write.mode(SaveMode.Append).format("hudi")
      .option("hoodie.insert.shuffle.parallelism", "100")
      .option("hoodie.upsert.shuffle.parallelism", "100")
      .option(PRECOMBINE_FIELD.key(), "ts")
      .option(RECORDKEY_FIELD.key(), "uuid")
      .option(PARTITIONPATH_FIELD.key(), "partitionPath")
      .option(TBL_NAME.key(), table)
      .save(path)
    // 打印总条数
    val showDF = spark.read.format("hudi").load(path)
    println("当前数据：" + showDF.count())
    showDF.createOrReplaceTempView("on_time")
    spark.sql("SELECT Year,Month, count(*) AS c1 FROM on_time GROUP BY Year,Month").show()
  }


  @Test
  def app(): Unit = {
     saveDataToS3(getS3SparkInstance, TABLE_NAME, BASE_PATH)
    // saveDataToLocal(getSparkInstance)
//    saveJsonDataToLocal(getSparkInstance)
  }

}
