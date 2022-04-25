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

import java.net.URI

import com.alibaba.fastjson.JSON._
import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.hudi.common.model.HoodieTableType
import org.junit.Test

import scala.io.Source


class Tester {
  @Test
  def checkConfig(): Unit = {
    val conf = ConfigFactory.load()
    println(conf.getString("hudi.storage.s3.endpoint"))
    println(conf.getString("hudi.storage.s3.accessKey"))
    println(conf.getString("hudi.storage.s3.secretKey"))
    println(conf.getString("hudi.storage.s3.enableSSL"))
  }

  @Test
  def testJsonFile(): Unit = {
    val conf = ConfigFactory.load()
    val filePath = conf.getString("test.json.path")
    //以指定的UTF-8字符集读取文件，第一个参数可以是字符串或者是java.io.File
    val source = Source.fromFile(filePath, "UTF-8")
    val content = source.mkString
    source.close()
    println(content)
  }

  @Test
  def jsonParser(): Unit = {
    val conf = ConfigFactory.load()
    val filePath = conf.getString("test.json.path")
    //以指定的UTF-8字符集读取文件，第一个参数可以是字符串或者是java.io.File
    val source = Source.fromFile(filePath, "UTF-8")
    val content = source.mkString
    source.close()
    var strList = List.empty[String]
    val array: JSONArray = parseArray(content)
    for (i <- 0 until array.size()) {
      val obj: JSONObject = array.getJSONObject(i)
      // 取得_source内容并拼接到strList
      strList = strList :+ obj.toString
    }
    println(strList)
  }

  @Test
  def readFile2(): Unit = {
    println(SparkHudiUtils.readFileContentOnResource("/json/ontime.json"))
  }


  @Test
  def validatorS3AAuth(): Unit = {
    val property = ConfigFactory.load()
    val s3a: S3AFileSystem = new S3AFileSystem()
    val uri: URI = new URI(property.getString("hudi.storage.s3.defaultFS"))
    val config: Configuration = new Configuration()
    config.set("fs.s3a.access.key", property.getString("hudi.storage.s3.accessKey"))
    config.set("fs.s3a.secret.key", property.getString("hudi.storage.s3.secretKey"))
    config.set("fs.s3a.endpoint", property.getString("hudi.storage.s3.endpoint"))
    config.set("fs.s3a.connection.ssl.enabled", property.getString("hudi.storage.s3.enableSSL"))
    config.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    config.set("fs.s3a.server-side-encryption-algorithm", "S3SignerType")
    s3a.initialize(uri, config)
  }

  @Test
  def tableType(): Unit = {
    println(HoodieTableType.MERGE_ON_READ.name())
    println(HoodieTableType.COPY_ON_WRITE.name())
  }

}
