package com.hjl.scheduler.report.media

import java.util.Properties

import com.hjl.constant.ReportConstant
import com.hjl.scheduler.JobComputing
import com.hjl.utils.JedisConnectionPool
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import redis.clients.jedis.Jedis

/**
  *
  * @author jiale.he
  * @date 2019/04/12
  * @email jiale.he@mail.hypers.com
  */
object MediaReport extends JobComputing {
  def main(args: Array[String]): Unit = {

    initAll(this.getClass.getName, true)

    val sourceDf: DataFrame = sqlContext.read.parquet(ReportConstant.PARQUET_SOURCE_PATH)
    val appRow: RDD[Row] = sourceDf.mapPartitions(partition => {
      val jedis: Jedis = JedisConnectionPool.getConnect
      partition.map(row => {
        val appid: String = row.getAs[String]("appid")
        val appname: String = jedis.hget("appdir", appid)
        Row(appid, appname)
      })
    })
    val schema = StructType(Array(
      StructField("appid", StringType, nullable = true),
      StructField("appname", StringType, nullable = true)
    ))

    val appDF: DataFrame = sqlContext.createDataFrame(appRow, schema).coalesce(20)

    val resultDF = sourceDf.coalesce(20).drop("appname").join(appDF, "appid")

    resultDF.registerTempTable(ReportConstant.TEMP)

    val sql: String =
      """
        |select appname,
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
        |group by appname
      """.stripMargin

    val resDF: DataFrame = sqlContext.sql(sql)

    val prop: Properties = getMysqlProperties()
    resDF.coalesce(1).write.mode(SaveMode.Overwrite).json(ReportConstant.MEDIA_SINK_JSON_PATH)
    resDF.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), ReportConstant.MEDIA_SINK_TABLE_NAME, prop)

    stopSparkContext()

  }
}
