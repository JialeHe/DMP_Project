package com.hjl.utils

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object LongAndLat2Baidu {
  def main(args: Array[String]): Unit = {
    // 模拟企业开发模式，首先判断一下目录是否为空
    if(args.length != 1) {
      println("目录不正确，退出程序！")
      sys.exit()
    }
    // 创建一个集合,存储一下输入输出目录
    val Array(inputPath) = args
    val conf = new SparkConf()
      .setAppName(this.getClass.getName).setMaster("local[2]")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.io.compression.snappy.codec","snappy")

    // 拿到数据经纬度
    sqlContext.read.parquet(inputPath).select("lat","long")
      .filter(
        """
          |cast(long as double) >=73 and cast(long as double) <=136 and
          |cast(lat as double) >=3 and cast(lat as double) <54
        """.stripMargin
      ).distinct()
      .foreachPartition(f => {
        val jedis = JedisConnectionPool.getConnect
        f.foreach(f => {
          val long = f.getAs[String]("long")
          val lat = f.getAs[String]("lat")
          // 通过百度的逆地址解析,获取到商圈信息
          val geoHash: String =
            GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble,long.toDouble,8)
          // 进行SN校验
          val baiduSN = BaiduLBSHandler.parseBusinessTagBy(long,lat)
          jedis.set(geoHash,baiduSN)
        })
        jedis.close()
      })
    sc.stop()
  }
}
