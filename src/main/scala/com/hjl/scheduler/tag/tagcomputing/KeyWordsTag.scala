package com.hjl.scheduler.tag.tagcomputing

import com.hjl.constant.TagConstant.{K, KEYWORDS}
import com.hjl.scheduler.tag.Tag
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  *
  * @author jiale.he
  * @date 2019/04/25
  * @email jiale.he@mail.hypers.com
  */
object KeyWordsTag extends Tag {
  /**
    * 打标签方法
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    // 获取stopWord广播变量
    val broadcast = args(1).asInstanceOf[Broadcast[Map[String, Int]]]
    val row = args(0).asInstanceOf[Row]
    var list = List[(String, Int)]()

    val keywords: String = row.getAs[String](KEYWORDS)
    keywords
      .split("\\|")
      .filter(word => {
        !broadcast.value.keySet.contains(word) && StringUtils.isNotBlank(word) && word.length <= 10
      }).foreach(word => {
      list :+= (K.concat(word), 1)
    })
    list
  }
}
