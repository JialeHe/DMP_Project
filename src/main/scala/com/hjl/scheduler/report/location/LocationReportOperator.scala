package com.hjl.scheduler.report.location

import com.hjl.scheduler.JobComputing
import com.hjl.utils.LocationUtil
import org.apache.spark.sql.DataFrame

/**
  *
  * @author jiale.he
  * @date 2019/04/09
  * @email jiale.he@mail.hypers.com
  */
object LocationReportOperator extends JobComputing{
  def main(args: Array[String]): Unit = {
    checkParam(args,2)
    val Array(inputPath, outputPath) = args
    initAll(this.getClass.getName,true)

    val sourceDF: DataFrame = sqlContext.read.parquet(inputPath)

    sourceDF.map(row => {
      // 从row中获取计算需要的参数
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

      val requestList = LocationUtil.requestUtil(requestmode, processnode)
      val adList = LocationUtil.adDspUtil(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)
      val clickList = LocationUtil.showClickUtil(requestmode, iseffective)

      ((provincename, cityname), requestList++adList++clickList)
    })
      .reduceByKey((list1, list2) => {
        list1.zip(list2).map(t => t._1+t._2)
      }).map(t => t._1._1+","+t._1._2+","+t._2.mkString(","))
      .saveAsTextFile(outputPath)

    stopSparkContext()
  }
}
