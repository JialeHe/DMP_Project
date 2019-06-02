package com.hjl.scheduler.report

import com.hjl.constant.CommonConstant
import com.hjl.scheduler.JobComputing
import com.hjl.utils.JedisConnectionPool
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

/**
  *
  * @author jiale.he
  * @date 2019/04/12
  * @email jiale.he@mail.hypers.com
  */
class App2Redis extends JobComputing {

  override def process(): Unit = {
    initAll(this.getClass.getName)
    // 获取字典文件数据
    val dirRDD: RDD[String] = sc.textFile(CommonConstant.APPDIR_SOURCE_PATH)
    dirRDD.map(_.split("\t", -1))
      .filter(_.length >= 5)
      .map(arr => (arr(4).trim, arr(1).trim))
      .foreachPartition(partition => {
        // 一个partition使用一个jedis连接对象
        val jedis: Jedis = JedisConnectionPool.getConnect
        partition.foreach(data => {
          jedis.hset(CommonConstant.APPDIR_KEY, data._1, data._2)
        })
        jedis.close()
      })
    stopSparkContext()
  }
}
