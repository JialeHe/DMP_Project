package com.hjl.scheduler.report.terminal

import java.util.Properties

import com.hjl.constant.ReportConstant
import com.hjl.scheduler.JobComputing
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  *
  * @author jiale.he
  * @date 2019/04/12
  * @email jiale.he@mail.hypers.com
  */
object NetWorkReport extends JobComputing{
  def main(args: Array[String]): Unit = {
    checkParam(args, 2)
    initAll(this.getClass.getName, true)
    val Array(inputPath, outputPath) = args

    val sourceDf: DataFrame = sqlContext.read.parquet(inputPath)
    sourceDf.registerTempTable(ReportConstant.TEMP)
    val sql: String =
      """
        |select networkmannername,
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
        |group by networkmannername
      """.stripMargin

    val resultDF: DataFrame = sqlContext.sql(sql)

    val prop: Properties = getMysqlProperties()

    resultDF.repartition(1).write.mode(SaveMode.Overwrite).json(outputPath)
    resultDF.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),ReportConstant.NETWORK_SINK_TABLE_NAME,prop)

    stopSparkContext()
  }
}
