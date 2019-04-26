package com


import ch.hsr.geohash.GeoHash
import com.hjl.constant.CommonConstant
import com.hjl.scheduler.JobComputing
import com.hjl.utils.BaiduLBSHandler
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.{immutable, mutable}

/**
  *
  * @author jiale.he
  * @date 2019/04/16
  * @email jiale.he@mail.hypers.com
  */
object DFTest extends JobComputing {
  def main(args: Array[String]): Unit = {
    initAll(this.getClass.getName)

    val sourceDf: DataFrame = sqlContext.read.parquet(CommonConstant.PARQUET_SOURCE_PATH)
    sourceDf.select("lat", "long")
      .filter(
        """
          |cast(long as double) >=73 and cast(long as double) <=136 and
          |cast(lat as double) >=3 and cast(lat as double) <54
        """.stripMargin
      ).distinct()
      .head(10)
      .map(row => {
        val long = row.getAs[String]("long")
        val lat = row.getAs[String]("lat")
        val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble,long.toDouble,8)
        val baiduSN = BaiduLBSHandler.parseBusinessTagBy(long,lat)
        (geoHash, baiduSN)
      }).foreach(println)

    //stopSparkContext()
  }

  def checkRowToMap(row: Row, key: String): immutable.Map[String, String] = {
    try {
      row.getAs[immutable.Map[String, String]](key)
    } catch {
      case _: Exception => null
    }
  }

  def mapValueStringToDecimal(oldMap: immutable.Map[String, String]): mutable.Map[String, BigDecimal] = {
    val newMap = mutable.Map[String, BigDecimal]()
    if (oldMap == null) {
      null
    } else {
      for ((k, v) <- oldMap) {
        if (v != "" && v != null) {
          newMap.put(k, stringToDecimal(v.trim))
        }
      }
      newMap
    }
  }

  def stringToDecimal(str: String): BigDecimal = {
    try {
      BigDecimal(str)
    } catch {
      case _: Exception => null
    }
  }

  def stringToInt(str: String) = {
    try {
      str.toInt
    } catch {
      case _: Exception => null
    }
  }

  def checkRow(row: Row, key: String): String = {
    try {
      row.getAs[String](key)
    } catch {
      case _: Exception => null
    }
  }
}
