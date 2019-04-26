package com.hjl.scheduler.tag.tagcomputing

import com.hjl.constant.TagConstant.{ADPLATFORMPROVIDERID, CN, DEFAULT_VALUE}
import com.hjl.scheduler.tag.Tag
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  *
  * @author jiale.he
  * @date 2019/04/16
  * @email jiale.he@mail.hypers.com
  */
object AdplatformproviderTag extends Tag {
  /**
    * 渠道标签
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String, Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val adplatformproviderid = row.getAs[Int](ADPLATFORMPROVIDERID)
    if (StringUtils.isBlank(adplatformproviderid.toString)) {
      list :+= (CN.concat(DEFAULT_VALUE), 1)
    }
    list :+= (CN.concat(adplatformproviderid.toString), 1)
    list
  }
}
