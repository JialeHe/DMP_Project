package com.hjl.scheduler.tag

import ch.hsr.geohash.GeoHash
import com.hjl.constant.{CommonConstant, TagConstant}
import com.hjl.scheduler.JobComputing
import com.hjl.utils.{BaiduLBSHandler, JedisConnectionPool}
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.Jedis

/**
  *
  * @author jiale.he
  * @date 2019/04/25
  * @email jiale.he@mail.hypers.com
  */
class Baidu2Redis extends JobComputing {
  private val logger: Logger = LoggerFactory.getLogger(classOf[Baidu2Redis])

  override def process(): Unit = {
    initAll(this.getClass.getName, flag = true)
    sqlContext.read.parquet(CommonConstant.PARQUET_SOURCE_PATH)
      .select(TagConstant.LONG, TagConstant.LAT)
      .filter(
        """
          |cast(long as double) >=73 and cast(long as double) <=136 and
          |cast(lat as double) >=3 and cast(lat as double) <54
        """.stripMargin)
      .distinct()
      .repartition(50)
      //      .repartition(100)
      .foreachPartition(partition => {
      val jedis: Jedis = JedisConnectionPool.getConnect
      partition.foreach(row => {
        val long = row.getAs[String](TagConstant.LONG)
        val lat = row.getAs[String](TagConstant.LAT)
        val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble, long.toDouble, 8)
        val baiduSN = BaiduLBSHandler.parseBusinessTagBy(long, lat)
        jedis.hset("tag2", geoHash, baiduSN)
      })
      jedis.close()
    })
    stopSparkContext()
  }
}
