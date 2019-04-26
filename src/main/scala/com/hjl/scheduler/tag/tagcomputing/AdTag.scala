package com.hjl.scheduler.tag.tagcomputing

import com.hjl.constant.TagConstant.{ADSPACETYPE, ADSPACETYPENAME, LC, LN}
import com.hjl.scheduler.tag.Tag
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  *
  * @author jiale.he
  * @date 2019/04/16
  * @email jiale.he@mail.hypers.com
  */
object AdTag extends Tag {
  /**
    * 打标签方法
    *
    * @param args Row
    * @return List((LN12,1), (LN视频前贴片,1))
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String, Int)]()
    // 参数强转
    val row: Row = args(0).asInstanceOf[Row]
    /**
      * 1)	广告位类型（标签格式： LCXX->1 或者 LCXX->1）xx 为数字(adspacetype)，
      * 小于 10 补 0，把广告位类型名称，LNxx->1(adspacetypename)
      * 获取广告参数
      * adspacetype: Int,
      * adspacetypename: String,
      */
    val adType: Int = row.getAs[Int](ADSPACETYPE)
    // 广告类型
    adType match {
      case a if a > 9 => list :+= (LN + a, 1)
      case a if a > 0 && a <= 9 => list :+= (LC + a, 1)
    }
    // 广告类型名称
    val adName: String = row.getAs[String](ADSPACETYPENAME)
    if (StringUtils.isNotBlank(adName)) {
      list :+= (LN + adName, 1)
    }
    list
  }

}
