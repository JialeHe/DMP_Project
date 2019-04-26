package com.hjl.scheduler.tag.tagcomputing

import com.hjl.constant.TagConstant._
import com.hjl.scheduler.tag.Tag
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  *
  * @author jiale.he
  * @date 2019/04/16
  * @email jiale.he@mail.hypers.com
  */
object AppTag extends Tag {
  /**
    * AppName标签
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String, Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    // 获取广播的字典文件
    val appdirBroadcast = args(1).asInstanceOf[Broadcast[Map[String, String]]]
    val appid: String = row.getAs[String](APPID)
    var appname: String = row.getAs[String](APPNAME)
    if (StringUtils.isBlank(appname) || StringUtils.contains(appname, SEARCH_STR)) {
      appname = appdirBroadcast.value.getOrElse(appid, DEFAULT_VALUE)
    }
    list :+= (APP.concat(appname),1)
    list
  }
}
