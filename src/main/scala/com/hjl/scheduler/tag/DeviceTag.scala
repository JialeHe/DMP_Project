package com.hjl.scheduler.tag

import com.hjl.constant.TagConstant._
import org.apache.spark.sql.Row

/**
  *
  * @author jiale.he
  * @date 2019/04/16
  * @email jiale.he@mail.hypers.com
  */
object DeviceTag extends Tag {
  /**
    * 设备标签
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String, Int)]()
    val row: Row = args(0).asInstanceOf[Row]

    /**
      * 设备操作系统标签
      * 设备操作系统
      * 1 Android D00010001
      * 2 IOS D00010002
      * 3 WinPhone D00010003
      * _ 其 他 D00010004
      * （1：android 2：ios 3：wp）
      */
    val client: Int = row.getAs[Int](CLIENT)
    client match {
      case 1 => list :+= (ANDROIDOS, 1)
      case 2 => list :+= (IOSOS, 1)
      case 3 => list :+= (WINOS, 1)
      case _ => list :+= (OTHEROS, 1)
    }

    /**
      * 设 备 联 网 方 式
      * WIFI D00020001
      * 4G D00020002
      * 3G D00020003
      * 2G D00020004
      * _ D00020005
      *
      * networkmannerid: Int,	联网方式 id
      * networkmannername: String,	联网方式名称
      */
    val networkmannername: String = row.getAs[String](NETWORKMANNERNAME)
    networkmannername match {
      case WIFIS => list :+= (WIFIS, 1)
      case FOURGS => list :+= (FOURG, 1)
      case THREEGS => list :+= (THREEG, 1)
      case TWOGS => list :+= (TWOG, 1)
      case _ => list :+= (OTHERG, 1)
    }

    /**
      * 设备运营商方式
      * 移 动 D00030001
      * 联 通 D00030002
      * 电 信 D00030003
      * _ D00030004
      *
      * ispid: Int,	运营商 id
      * ispname: String,	运营商名称
      */
    val ispname: String = row.getAs[String](ISPNAME)
    ispname match {
      case MOVES => list :+= (MOVE, 1)
      case UNICOMS => list :+= (UNICOM, 1)
      case TELECOMs => list :+= (TELECOM, 1)
      case _ => list :+= (OTHERCOM, 1)
    }
    list
  }
}
