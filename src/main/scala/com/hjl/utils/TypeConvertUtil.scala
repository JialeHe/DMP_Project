package com.hjl.utils

/**
  * 数据格式转换
  *
  * @author jiale.he
  * @date 2019/04/09
  * @email jiale.he@mail.hypers.com
  */
object TypeConvertUtil {

  // String转换Int
  def s2I(str: String): Int = {
    try {
      str.toInt
    } catch {
      case _: Exception => 0
    }
  }

  // String转换Double
  def s2D(str: String): Double = {
    try {
      str.toDouble
    } catch {
      case _: Exception => 0
    }
  }

  // check String
  def s2s(str: String): String = {
    if (str == null) {
      "null"
    } else {
      str
    }
  }
}
