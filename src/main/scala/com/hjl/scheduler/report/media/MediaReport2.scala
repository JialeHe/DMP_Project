package com.hjl.scheduler.report.media

import java.util.Properties

import com.hjl.constant.ReportConstant
import com.hjl.scheduler.JobComputing
import com.hjl.utils.{JedisConnectionPool, LocationUtil}
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import redis.clients.jedis.Jedis

/**
  *
  * @author jiale.he
  * @date 2019/04/15
  * @email jiale.he@mail.hypers.com
  */
object MediaReport2 extends JobComputing {
  def main(args: Array[String]): Unit = {

    initAll(this.getClass.getName, flag = true)

    val sourceDf: DataFrame = sqlContext.read.parquet(ReportConstant.PARQUET_SOURCE_PATH)

    val listRDD: RDD[(String, List[Double])] = sourceDf.mapPartitions(partition => {
      val jedis: Jedis = JedisConnectionPool.getConnect
      partition.map(row => {
        var appname: String = row.getAs[String]("appname")
        val appid = row.getAs[String]("appid")
        if (StringUtils.isBlank(appname)) {
          appname = jedis.hget("appdir", appid)
        }
        val provincename = row.getAs[String]("provincename")
        val cityname = row.getAs[String]("cityname")
        val requestmode = row.getAs[Int]("requestmode")
        val processnode = row.getAs[Int]("processnode")
        val iseffective = row.getAs[Int]("iseffective")
        val isbilling = row.getAs[Int]("isbilling")
        val isbid = row.getAs[Int]("isbid")
        val iswin = row.getAs[Int]("iswin")
        val adorderid = row.getAs[Int]("adorderid")
        val winprice = row.getAs[Double]("winprice")
        val adpayment = row.getAs[Double]("adpayment")
        val reqlist: List[Double] = LocationUtil.requestUtil(requestmode, processnode)
        val showlist: List[Double] = LocationUtil.showClickUtil(requestmode, iseffective)
        val adlist: List[Double] = LocationUtil.adDspUtil(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

        (appname, reqlist ++ showlist ++ adlist)
      })
    })
    val sumRDD: RDD[(String, List[Double])] = listRDD.reduceByKey((list1, list2) => {
      list1.zip(list2).map(list => list._1 + list._2)
    })

    val rowRDD: RDD[Row] = sumRDD.map(data => {
      val list: List[Double] = data._2
      Row(data._1, list.head, list(1), list(2), list(3), list(4), list(5), list(6), list(7), list(8))
    })
    val schema = StructType(Array(
      StructField("appname", StringType, nullable = true),
      StructField("ysrequest", DoubleType, nullable = true),
      StructField("yxrequesy", DoubleType, nullable = true),
      StructField("adrequest", DoubleType, nullable = true),
      StructField("cybid", DoubleType, nullable = true),
      StructField("bidsuccess", DoubleType, nullable = true),
      StructField("shows", DoubleType, nullable = true),
      StructField("clicks", DoubleType, nullable = true),
      StructField("dspsonsumet", DoubleType, nullable = true),
      StructField("dscost", DoubleType, nullable = true)
    ))
    val resultDF: DataFrame = sqlContext.createDataFrame(rowRDD, schema)

    val prop: Properties = getMysqlProperties()
    resultDF.coalesce(1).write.mode(SaveMode.Overwrite).json(ReportConstant.MEDIA_SINK_JSON_PATH)
    resultDF.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), ReportConstant.MEDIA_SINK_TABLE_NAME, prop)

    stopSparkContext()

  }
}
