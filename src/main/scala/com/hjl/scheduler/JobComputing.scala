package com.hjl.scheduler

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
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
  var load: Config = _

  def getMysqlProperties(): Properties = {
    load = ConfigFactory.load
    val prop = new Properties()
    prop.put("user", load.getString("jdbc.user"))
    prop.put("password", load.getString("jdbc.password"))
    prop.put("driver", load.getString("jdbc.driver"))
    prop
  }

  def checkParam(args: Array[String], num: Int): Unit = {
    if (args.length != num) {
      println("目录参数不正确,退出程序!")
      sys.exit()
    }
  }

  def initAll(appName: String, flag: Boolean = false): Unit = {
    if (!flag) {
      initSparkConf(appName)
      initSparkContext(appName)
      initSQLContext()
    } else {
      initSparkConf(appName, true)
      initSparkContext(appName)
      initSQLContext(true)
    }
  }

  def initSQLContext(flag: Boolean = false) = {
    if (!flag) {
      sqlContext = new SQLContext(sc)
    } else {
      sqlContext = new SQLContext(sc)
      sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")
    }
  }

  def stopSparkContext(): Unit = {
    sc.stop()
  }

  def initSparkContext(appName: String): Unit = {
    sc = new SparkContext(conf)
  }


  def initSparkConf(appName: String, flag: Boolean = false): Unit = {
    if (!flag) {
      conf = new SparkConf().setAppName(appName)
        .setMaster("local[*]")
    } else {
      conf = new SparkConf()
        .setAppName(appName)
//        .setMaster("local[*]")
        //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    }
  }

}
