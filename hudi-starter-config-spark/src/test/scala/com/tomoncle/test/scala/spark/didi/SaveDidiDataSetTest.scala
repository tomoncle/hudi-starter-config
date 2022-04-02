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

package com.tomoncle.test.scala.spark.didi

import com.tomoncle.test.scala.spark.SparkHudiUtils
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.junit.Test
import org.slf4j.{Logger, LoggerFactory}

/**
  * 滴滴海口出行运营数据分析，使用SparkSQL操作数据，先读取CSv文件，保存至Hudi表中
  */
class SaveDidiDataSetTest {

  protected lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  @Test
  def app(): Unit = {
    logger.debug("测试滴滴数据集：")
    // 构建SparkSession实例对象（集成Hudi和HDFS
    val spark: SparkSession = SparkHudiUtils.getSparkInstance
    // 加载本地CSV文件格式滴滴出行数据
    val dataPath = SparkHudiUtils.getConfigValue("hudi.dataset.load.path")
    val didiDF = SparkHudiUtils.readCsvFileToDataFrame(spark, dataPath)
    didiDF.printSchema()
    didiDF.show(10, truncate = false)
    // 滴滴出行数据ETL处理
    val etlDF: DataFrame = dataFramePipeline(didiDF)
    etlDF.printSchema()
    etlDF.show(10, truncate = false)
    // 保存转换后数据至Hudi表
    val table = "didi_row"
    val path = SparkHudiUtils.getConfigValue("hudi.storage.local.dir") + "/didi_data/" + table
    saveDataSetToLocal(etlDF, table, path)
    // 应用结束关闭资源
    spark.stop()
  }

  /**
    * 对滴滴出行海口数据进行ETL转换操作：指定ts和partitionPath列
    */
  def dataFramePipeline(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    // partitionPath: Hudi表分区字段，三级分区 -> yyyy/MM/dd ; 一级分区 -> yyyy-MM-dd
    // ts: Hudi表记录数据合并字段，使用发车时间
    val partitionPath: Column = concat_ws("/", col("year"), col("month"), col("day"))
    val ts: Column = unix_timestamp(col("departure_time"), "yyyy-MM-dd HH:mm:ss")
    df.withColumn("partitionPath", partitionPath)
      .withColumn("ts", ts)
      .drop("year", "month", "day") // 删除列
  }

  /**
    * 将数据集DataFrame保存至Hudi表中，表的类型为COW，属于批量保存数据，写少读多
    */
  def saveDataSetToLocal(df: DataFrame, table: String, path: String): Unit = {
    // 导入包
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.config.HoodieWriteConfig._

    // 保存数据
    df.write
      .mode(SaveMode.Overwrite)
      .format("hudi")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option(RECORDKEY_FIELD.key(), "order_id")
      .option(PRECOMBINE_FIELD.key(), "ts")
      .option(PARTITIONPATH_FIELD.key(), "partitionPath")
      .option(TBL_NAME.key(), table)
      .save(path)
  }

}
