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

import java.text.SimpleDateFormat
import java.util.Date
import java.util.UUID.randomUUID

import com.alibaba.fastjson.JSON.parseArray
import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.io.Source

object SparkHudiUtils {

  private lazy val MINE_HADOOP_HOME: String = "D:\\software\\installs\\apache-hadoop-2.7.3"

  def sayHello(): String = {
    "hello world!"
  }

  /**
    * 获取默认的存储系统
    *
    * @return
    */
  def getHudiDefaultFs: String = {
    val conf = ConfigFactory.load()
    conf.getString("hudi.storage.s3.defaultFS")
  }

  /**
    * 配置spark上下文，支持 S3对象存储
    * 文档： https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#General_S3A_Client_configuration
    *
    * @param spark SparkSession
    */
  def initSparkContextForS3(spark: SparkSession): Unit = {
    val conf = ConfigFactory.load()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", conf.getString("hudi.storage.s3.accessKey"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", conf.getString("hudi.storage.s3.secretKey"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", conf.getString("hudi.storage.s3.endpoint"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", conf.getString("hudi.storage.s3.enableSSL"))
    spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.signing-algorithm", "S3SignerType")
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.server-side-encryption-algorithm", "S3SignerType")
    // 对S3兼容存储的所有请求使用路径式访问。此属性针对不支持虚拟主机式访问的S3兼容存储。（默认为false）
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "false")
  }

  def setDefaultEnv(): Unit = {
    // 获取默认环境变量，如果没用使用配置的值
    val hadoopDir = scala.util.Properties.envOrElse("HADOOP_HOME", MINE_HADOOP_HOME)
    // 配置当前环境变量
    System.setProperty("hadoop.home.dir", hadoopDir)
    // WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
    System.setProperty("HADOOP_COMMON_LIB_NATIVE_DIR", hadoopDir + "\\lib\\native")
    System.setProperty("HADOOP_OPTS", "-Djava.library.path=" + hadoopDir + "\\lib\\native")
  }

  /**
    * 获取Spark对象
    *
    * @return SparkSession
    */
  def getSparkInstance: SparkSession = {
    setDefaultEnv()
    // 初始化 SparkSession
    val spark: SparkSession = {
      SparkSession.builder()
        .appName(this.getClass.getSimpleName.stripSuffix("$"))
        .master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    }
    spark
  }

  /**
    * 获取加了了对象存储配置的Spark对象
    *
    * @return SparkSession
    */
  def getS3SparkInstance: SparkSession = {
    setDefaultEnv()
    // 初始化 SparkSession
    val spark: SparkSession = {
      SparkSession.builder()
        .appName(this.getClass.getSimpleName.stripSuffix("$"))
        .master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    }
    initSparkContextForS3(spark)
    spark
  }

  /**
    * 关闭spark对象
    *
    * @param spark SparkSession
    */
  def stopSparkInstance(spark: SparkSession): Unit = {
    spark.stop()
  }

  /**
    * 缓存 DataFrame
    *
    * @param df DataFrame
    */
  def cacheDataFrame(df: DataFrame): Unit = {
    // 如果数据被使用多次，建议缓存
    df.persist(StorageLevel.MEMORY_AND_DISK)
  }

  /**
    * 释放缓存 DataFrame
    *
    * @param df DataFrame
    */
  def clearDataFrame(df: DataFrame): Unit = {
    // 当数据不在使用时，释放缓存
    df.unpersist()
  }

  /**
    * 读取文件内容
    *
    * @param filePath 文件路径
    * @return 文件内容
    */
  def readFileContent(filePath: String): String = {
    //以指定的UTF-8字符集读取文件，第一个参数可以是字符串或者是java.io.File
    val source = Source.fromFile(filePath, "UTF-8")
    val content = source.mkString
    source.close()
    content
  }

  /**
    * 从resources目录下读取文件内容
    *
    * @param filePath 文件路径
    * @return 文件内容
    */
  def readFileContentOnResource(filePath: String): String = {
    val file = Source.fromURL(getClass.getResource(filePath))
    val content = file.mkString
    file.close()
    content
  }

  /**
    * 获取 resources 目录下所有配置文件的配置信息
    *
    * @param key key
    * @return value
    */
  def getConfigValue(key: String): String = {
    val conf = ConfigFactory.load()
    conf.getString(key)
  }


  /**
    *
    * @param filePath json文件路径, 文件内容必须是json数组
    * @param spark    SparkSession
    * @return Dataset[String]
    */
  def loadJsonFileToDataset(filePath: String, spark: SparkSession): Dataset[String] = {
    import spark.implicits._

    var strList = List.empty[String]
    val content = readFileContent(filePath)
    val array: JSONArray = parseArray(content)
    val partition = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
    for (i <- 0 until array.size()) {
      val obj: JSONObject = array.getJSONObject(i)
      // 给数据添加 主键，时间，分区等字段
      obj.putIfAbsent("uuid", randomUUID().toString.replaceAll("-", ""))
      obj.putIfAbsent("ts", System.currentTimeMillis().toString)
      obj.putIfAbsent("partitionPath", partition)
      strList = strList :+ obj.toString
    }
    strList.toDS()
  }

  /**
    * 读取CSV格式文本文件数据，封装到DataFrame中
    */
  def readCsvFileToDataFrame(spark: SparkSession, path: String): DataFrame = {
    spark.read.option("sep", "\\t") // 设置分隔符为制表符
      .option("header", "true") // 文件首行为列名称
      .option("inferSchema", "true") // 依据数值自动推断数据类型
      .csv(path) // 指定文件路径
  }
}
