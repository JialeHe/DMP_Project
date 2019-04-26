package com.hjl.scheduler.tag

import com.hjl.constant.CommonConstant
import com.hjl.scheduler.JobComputing
import com.hjl.scheduler.tag.tagcomputing._
import com.hjl.utils.TagUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame

/**
  * 合并标签
  *
  * @author jiale.he
  * @date 2019/04/15
  * @email jiale.he@mail.hypers.com
  */
object TagContext extends JobComputing {
  def main(args: Array[String]): Unit = {
    initAll(this.getClass.getName, flag = true)

    // 获取appdir广播
    val appdirMap: Map[String, String] = sc.textFile(CommonConstant.APPDIR_SOURCE_PATH)
      .map(_.split("\t", -1))
      .filter(_.length >= 5)
      .map(arr => (arr(4), arr(1)))
      .collect
      .toMap
    // 广播app字典文件
    val appBroacast: Broadcast[Map[String, String]] = sc.broadcast(appdirMap)

    // 获取StopWords
    val stopwords: Map[String, Int] = sc.textFile(CommonConstant.STOP_WORD_SOURCE_PATH)
      .map((_, 0))
      .collect
      .toMap
    val stopWordsBroadcast: Broadcast[Map[String, Int]] = sc.broadcast(stopwords)

    val sourceDF: DataFrame = sqlContext.read.parquet(CommonConstant.PARQUET_SOURCE_PATH)

    sourceDF
      .filter(TagUtil.UserId)
      .map(row => {
        // List(AOD: cd6e403047b20ff, TMM: eed790fade337966254d216059a17084)
        val userId = TagUtil.getAnyAllUserId(row)
        // 广告位标签 List((LC 9,1), (LN 视频暂停悬浮,1))
        val adTag: List[(String, Int)] = AdTag.makeTags(row)
        // appName标签 List((APP 爱奇艺,1))
        val appTag: List[(String, Int)] = AppTag.makeTags(row, appBroacast)
        // 渠道标签 List((CN 100018,1))
        val adplatTag: List[(String, Int)] = AdplatformproviderTag.makeTags(row)
        // 设备标签 List((Android D00010001,1), (2G D00020004,1), (电信 D00030003,1))
        val deviceTag: List[(String, Int)] = DeviceTag.makeTags(row)
        // 关键字标签 List((K 言情剧,1), (K 内地剧场,1), (K 家庭剧,1))
        val stopwordsTag: List[(String, Int)] = KeyWordsTag.makeTags(row, stopWordsBroadcast)
        //todo 地域标签
        val loccationTag: List[(String, Int)] = LocationTag.makeTags(row)
        //todo 商圈标签


        loccationTag
      }).foreach(println)

    stopSparkContext()
  }
}
