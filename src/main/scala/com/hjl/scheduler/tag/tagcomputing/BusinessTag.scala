package com.hjl.scheduler.tag.tagcomputing

import ch.hsr.geohash.GeoHash
import com.hjl.constant.TagConstant
import com.hjl.scheduler.tag.Tag
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.Jedis

/**
  *
  * @author jiale.he
  * @date 2019/05/24
  * @email jiale.he@mail.hypers.com
  */
object BusinessTag extends Tag{

  private val logger: Logger = LoggerFactory.getLogger(BusinessTag.getClass)

  /**
    * 打标签方法
    *
    * 商圈标签，从Redis数据库中获取经纬度对应的GEOHash值，根据GeoHash值来获取商圈信息
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String, Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val jedis = args(1).asInstanceOf[Jedis]
    val long = row.getAs[String](TagConstant.LONG)
    val lat = row.getAs[String](TagConstant.LAT)

    if(StringUtils.isNotBlank(long) && StringUtils.isNotBlank(lat))
    if(long.toDouble >= 73
      && long.toDouble <= 136
      && lat.toDouble >= 3
      && lat.toDouble <= 54){

      //取出经纬度
      val long = row.getAs[String]("long")
      val lat =  row.getAs[String]("lat")
      // 将经纬度转换为geohash码
      val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble,long.toDouble,8)
      // 进行取值
      val geo = jedis.hget(TagConstant.REDIS_TAG_HKEY,geoHash)
      if (StringUtils.isNotBlank(geo)){
        //获取对应的商圈
        geo.split(";").foreach(t => list:+=(t,1))
      }

    }
    list
  }
}
