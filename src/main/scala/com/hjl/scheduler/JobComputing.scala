package com.hjl.scheduler

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author jiale.he
  * @date 2019/04/09
  * @email jiale.he@mail.hypers.com
  */
class JobComputing {
  var conf: SparkConf = _
  var sc: SparkContext = _
  var sqlContext: SQLContext = _

  def checkParam(args: Array[String]): Unit ={
    if(args.length != 2){
      println("目录参数不正确,退出程序!")
      sys.exit()
    }
  }

  def initAll(appName: String, flag: Boolean= false): Unit ={
    if (!flag){
      initSparkConf(appName)
      initSparkContext(appName)
      initSQLContext()
    }else {
      initSparkConf(appName, true)
      initSparkContext(appName)
      initSQLContext(true)
    }
  }

  def initSQLContext(flag: Boolean = false) ={
    if (!flag){
      sqlContext = new SQLContext(sc)
    }else {
      sqlContext = new SQLContext(sc)
      sqlContext.setConf("spark.io.compression.codec","snappy")
    }
  }

  def stopSparkContext(): Unit ={
    sc.stop()
  }

  def initSparkContext(appName: String): Unit ={
    sc = new SparkContext(conf)
  }


  def initSparkConf(appName: String ,flag: Boolean = false): Unit ={
    if (!flag) {
      conf = new SparkConf().setAppName(appName).setMaster("local[2]")
    } else {
      conf = new SparkConf()
          .setAppName(appName)
          .setMaster("local[2]")
          .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    }
  }

}
