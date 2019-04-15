package com

import java.text.SimpleDateFormat

import com.hjl.scheduler.JobComputing

/**
  *
  * @author jiale.he
  * @date 2019/04/08
  * @email jiale.he@mail.hypers.com
  */
object GitTest extends JobComputing{
  def main(args: Array[String]): Unit = {

    val list1: List[Double] = List[Double](1,2,3,4)
    val list2: List[Double] = List[Double](0,0,0,0)

    val res1: List[Double] = list1++list2

    val list3: List[Double] = List[Double](1,2,3,4)
    val list4: List[Double] = List[Double](0,0,0,0)

    val res2: List[Double] = list3++list4

    println(res1)
    println(res2)

    val zip1: List[(Double, Double)] = res1.zip(res2)
    println(zip1)

    val map1: List[Double] = zip1.map(t => t._1 + t._2)
    println(map1)


  }


  def getJsonTime(timestamp: String): String = {
    if (timestamp == ""){
      ""
    }else{
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
