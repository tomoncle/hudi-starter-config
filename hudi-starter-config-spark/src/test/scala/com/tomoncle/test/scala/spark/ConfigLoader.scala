package com.tomoncle.test.scala.spark

import com.alibaba.fastjson.JSON._
import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.typesafe.config.ConfigFactory
import org.junit.Test

import scala.io.Source


class ConfigLoader {
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
  def jsonParser(): Unit ={
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
  def readFile2(): Unit ={
    println(SparkHudiUtils.readFileContentOnResource("/json/ontime.json"))
  }


}
