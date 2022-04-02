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

import java.util.{Calendar, Date}

import com.tomoncle.test.scala.spark.SparkHudiUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test

/**
  * 滴滴海口出行运营数据分析，使用SparkSQL操作数据，加载Hudi表数据，按照业务需求统计
  */
class AnalysisDidiDataSetTest {

  /**
    * 订单类型统计，字段：product_id
    */
  @Test
  def reportProduct(): Unit = {
    // 按照product_id分组统计
    val df: DataFrame = readFromHudiToDataFrame.groupBy("product_id").count()
    // 自定义UDF函数，productId转换为名称
    val getProductName = udf((productId: Int) => {
      productId match {
        case 1 => "滴滴专车"
        case 2 => "滴滴企业专车"
        case 3 => "滴滴快车"
        case 4 => "滴滴企业快车"
      }
    })
    // 转换列名
    val dataDF: DataFrame = df.select(
      getProductName(col("product_id")).as("order_type"),
      col("count").as("total"))
    dataDF.printSchema()
    dataDF.show(10, truncate = false)
  }

  /**
    * 订单时效性统计，字段：type
    */
  @Test
  def reportType(): Unit = {
    /// 按照type分组统计
    val reportDF: DataFrame = readFromHudiToDataFrame.groupBy("type").count()
    // 自定义UDF函数，转换名称
    val getTypeName = udf((realtimeType: Int) => {
      realtimeType match {
        case 0 => "实时"
        case 1 => "预约"
      }
    })
    // 转换列名
    val dataDF: DataFrame = reportDF.select(
      getTypeName(col("type")).as("realtimeType"),
      col("count").as("total")
    )
    dataDF.printSchema()
    dataDF.show(10, truncate = false)
  }

  /**
    * 交通类型统计，字段：traffic_type
    */
  @Test
  def reportTraffic(): Unit = {
    // 按照交通类型 traffic_type 分组统计即可
    val df: DataFrame = readFromHudiToDataFrame.groupBy("traffic_type").count()
    // 自定义UDF函数，转换名称
    val getTrafficTypeName = udf((trafficType: Int) => {
      trafficType match {
        case 0 => "普通散客"
        case 1 => "企业时租"
        case 2 => "企业接机套餐"
        case 3 => "企业送机套餐"
        case 4 => "拼车"
        case 5 => "接机"
        case 6 => "送机"
        case 302 => "跨城拼车"
        case _ => "未知"
      }
    })
    // 转换列名
    val dataDF: DataFrame = df.select(
      getTrafficTypeName(col("traffic_type")).as("trafficType"),
      col("count").as("total")
    )
    dataDF.printSchema()
    dataDF.show(10, truncate = false)
  }

  /**
    * 订单价格统计，先将订单价格划分阶段，再统计各个阶段数目，使用字段：pre_total_fee
    */
  @Test
  def reportPrice(): Unit = {
    val df: DataFrame = readFromHudiToDataFrame.agg(
      sum(when(col("pre_total_fee").leq(15), 1).otherwise(0)
      ).as("<=15"), // 价格 <= 15
      sum(when(col("pre_total_fee").between(16, 30), 1).otherwise(0)
      ).as("16-30"), // 价格 16 ~ 30
      sum(when(col("pre_total_fee").between(31, 50), 1).otherwise(0)
      ).as("31-50"), // 价格 31 ~ 50
      sum(when(col("pre_total_fee").between(51, 100), 1).otherwise(0)
      ).as("51-100"), // 价格 51 ~ 100
      sum(when(col("pre_total_fee").gt(100), 1).otherwise(0)
      ).as("100+") // 价格 100+
    )
    df.printSchema()
    df.show(10, truncate = false)
  }

  /**
    * 加载Hudi表数据
    *
    * @return DataFrame
    */
  def readFromHudiToDataFrame: DataFrame = {
    val spark: SparkSession = SparkHudiUtils.getSparkInstance
    val path = SparkHudiUtils.getConfigValue("hudi.storage.local.dir") + "/didi_data/didi_row"
    val df: DataFrame = spark.read.format("hudi").load(path)
    // 选择字段
    df.select("product_id", "type", "traffic_type", "pre_total_fee", "start_dest_distance", "departure_time")
  }

  /**
    * 订单距离统计，先将订单距离划分为不同区间，再统计各个区间数目，使用字段：start_dest_distance
    */
  @Test
  def reportDistance(): Unit = {
    val km = 1000
    val df: DataFrame = readFromHudiToDataFrame.agg(
      sum(when(col("start_dest_distance").leq(km * 10), 1).otherwise(0)
      ).as("0-10km"), // 距离： <= 10km
      sum(when(col("start_dest_distance").between(km * 10 + 1, km * 20), 1).otherwise(0)
      ).as("10-20km"), // 距离： 10 ~ 20km
      sum(when(col("start_dest_distance").between(km * 20 + 1, km * 30), 1).otherwise(0)
      ).as("20-30"), // 距离： 20 ~ 20km
      sum(when(col("start_dest_distance").between(km * 30 + 1, km * 50), 1).otherwise(0)
      ).as("30-50km"), // 距离： 30 ~ 50km
      sum(when(col("start_dest_distance").gt(km * 50 + 1), 1).otherwise(0)
      ).as("50+km") // 距离： 50km+
    )
    df.printSchema()
    df.show(10, truncate = false)
  }

  /**
    * 订单星期分组统计，先将日期转换为星期，再对星期分组统计，使用字段：departure_time
    */
  @Test
  def reportWeek(): Unit = {
    // 自定义UDF函数，转换日期为星期
    val dateToWeek: UserDefinedFunction = udf((dateStr: String) => {
      val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
      val calendar: Calendar = Calendar.getInstance()
      val date: Date = format.parse(dateStr)
      calendar.setTime(date)
      val dayWeek = calendar.get(Calendar.DAY_OF_WEEK) match {
        case 1 => "星期日"
        case 2 => "星期一"
        case 3 => "星期二"
        case 4 => "星期三"
        case 5 => "星期四"
        case 6 => "星期五"
        case 7 => "星期六"
      }
      dayWeek
    })
    // 使用udf函数对数据处理
    val dataDF: DataFrame = readFromHudiToDataFrame
      .select(dateToWeek(col("departure_time")).as("week"))
      .groupBy("week").count()
      .select(
        col("week"),
        col("count").as("total"))
      .orderBy(asc("total"))
    dataDF.printSchema()
    dataDF.show(10, truncate = false)
  }

}
