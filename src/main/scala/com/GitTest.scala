package com

import java.text.SimpleDateFormat

import com.hjl.scheduler.JobComputing

/**
  *
  * @author jiale.he
  * @date 2019/04/08
  * @email jiale.he@mail.hypers.com
  */
object GitTest extends JobComputing {
  def main(args: Array[String]): Unit = {
    val str = "10.1".toDouble
    val double: BigDecimal = BigDecimal(str)
    //println(str)
//    val newMap = scala.collection.mutable.HashMap[String, BigDecimal]()
//    newMap.put("hejiale", BigDecimal("  0 ".trim))
//    newMap.foreach(println)
    val string: String = true.toString
    println(string)
  }


  def getJsonTime(timestamp: String): String = {
    if (timestamp == "") {
      ""
    } else {
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      format.format(timestamp.toLong)
    }
  }

  def replaceParams(str: String, map: Map[String, String]): String = {
    if (map == null) str
    else {
      var result: String = str
      map.foreach(value => {
        result = result.replace(value._1, value._2)
      })
      result
    }
  }
}
