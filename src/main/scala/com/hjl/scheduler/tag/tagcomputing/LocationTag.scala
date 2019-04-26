package com.hjl.scheduler.tag.tagcomputing

import com.hjl.constant.TagConstant.{CITYNAME, PROVINCENAME, ZC, ZP}
import com.hjl.scheduler.tag.Tag
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  *
  * @author jiale.he
  * @date 2019/04/25
  * @email jiale.he@mail.hypers.com
  */
object LocationTag extends Tag {
  /**
    * 打标签方法
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list: List[(String, Int)] = List[(String, Int)]()
    val row: Row = args(0).asInstanceOf[Row]

    val provinceName: String = row.getAs[String](PROVINCENAME)
    val cityName: String = row.getAs[String](CITYNAME)

    if (StringUtils.isNotBlank(provinceName)) {
      list :+= (ZP.concat(provinceName), 1)
    }
    if (StringUtils.isNotBlank(cityName)) {
      list :+= (ZC.concat(cityName), 1)
    }
    list
  }
}
