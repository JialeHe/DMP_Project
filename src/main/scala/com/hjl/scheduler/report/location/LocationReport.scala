package com.hjl.scheduler.report.location

import java.util.Properties

import com.hjl.scheduler.JobComputing
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * 地域分布报表
  * 1. 将统计结果输出成json格式
  * 2. 将统计结果写到mysql
  * 3. 使用算子实现上诉统计，存储到磁盘
  *
  * @author jiale.he
  * @date 2019/04/09
  * @email jiale.he@mail.hypers.com
  */
object LocationReport extends JobComputing{
  def main(args: Array[String]): Unit = {
    checkParam(args, 2)
    val Array(inputPath, outputPath) = args
    initAll(this.getClass.getName,true)

    val sourceDF: DataFrame = sqlContext.read.parquet(inputPath)
    sourceDF.registerTempTable("temp")
    // requestmode	processnode	iseffective	isbilling	isbid	iswin	adordeerid
    val sql: String =
      """
        |select provincename, cityname,
        |sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) ysrequest,
        |sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) yxrequesy,
        |sum(case when requestmode=1 and processnode=3 then 1 else 0 end) adrequest,
        |sum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) cybid,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) bidsuccess,
        |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) shows,
        |sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) clicks,
        |sum(case when iseffective = 1 and isbilling = 1 then winprice/1000 else 0 end) dspsonsumet,
        |sum(case when iseffective = 1 and isbilling = 1 then adpayment/1000 else 0 end) dscost
        |from temp
        |group by provincename,cityname
      """.stripMargin

    val resultDF: DataFrame = sqlContext.sql(sql)

    val prop: Properties = getMysqlProperties()

    resultDF.repartition(1).write.mode(SaveMode.Append).json(outputPath)
    resultDF.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "location", prop)

    stopSparkContext()
  }
}