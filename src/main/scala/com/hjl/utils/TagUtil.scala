package com.hjl.utils

import com.hjl.constant.TagConstant._
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * 标签工具类
  *
  * @author jiale.he
  * @date 2019/04/16
  * @email jiale.he@mail.hypers.com
  */
object TagUtil {

  val UserId: String =
    s"""
       |$IMEI != '' or $MAC != '' or $IDFA != '' or $OPENUDID != '' or $ANDROIDID != '' or
       |$IMEIMD5 != '' or $MACMD5 != '' or $IDFAMD5 != '' or $OPENUDIDMD5 != '' or $ANDROIDIDMD5 != '' or
       |$IMEISHA1 != '' or $MACSHA1 != '' or $IDFASHA1 != '' or $OPENUDIDSHA1 != '' or $ANDROIDIDSHA1 != ''
    """.stripMargin

  def getAnyAllUserId(row: Row) = {
    var list = List[String]()
    if (StringUtils.isNotEmpty(row.getAs[String](IMEI))) {
      list :+= TM + row.getAs[String](IMEI)
    }
    if (StringUtils.isNotEmpty(row.getAs[String](MAC))) {
      list :+= MC + row.getAs[String](MAC)
    }
    if (StringUtils.isNotEmpty(row.getAs[String](IDFA))) {
      list :+= ID + row.getAs[String](IDFA)
    }
    if (StringUtils.isNotEmpty(row.getAs[String](ANDROIDID))) {
      list :+= AOD + row.getAs[String](ANDROIDID)
    }
    if (StringUtils.isNotEmpty(row.getAs[String](OPENUDID))) {
      list :+= OD + row.getAs[String](OPENUDID)
    }
    if (StringUtils.isNotEmpty(row.getAs[String](IMEIMD5))) {
      list :+= TMM + row.getAs[String](IMEIMD5)
    }
    if (StringUtils.isNotEmpty(row.getAs[String](MACMD5))) {
      list :+= MCM + row.getAs[String](MACMD5)
    }
    if (StringUtils.isNotEmpty(row.getAs[String](IDFAMD5))) {
      list :+= IDM + row.getAs[String](IDFAMD5)
    }
    if (StringUtils.isNotEmpty(row.getAs[String](ANDROIDIDMD5))) {
      list :+= AODM + row.getAs[String](ANDROIDIDMD5)
    }
    if (StringUtils.isNotEmpty(row.getAs[String](OPENUDIDMD5))) {
      list :+= ODM + row.getAs[String](OPENUDIDMD5)
    }
    if (StringUtils.isNotEmpty(row.getAs[String](IMEISHA1))) {
      list :+= TMS + row.getAs[String](IMEISHA1)
    }
    if (StringUtils.isNotEmpty(row.getAs[String](MACSHA1))) {
      list :+= MCS + row.getAs[String](MACSHA1)
    }
    if (StringUtils.isNotEmpty(row.getAs[String](IDFASHA1))) {
      list :+= IDS + row.getAs[String](IDFASHA1)
    }
    if (StringUtils.isNotEmpty(row.getAs[String](ANDROIDIDSHA1))) {
      list :+= AODS + row.getAs[String](ANDROIDIDSHA1)
    }
    if (StringUtils.isNotEmpty(row.getAs[String](OPENUDIDSHA1))) {
      list :+= ODS + row.getAs[String](OPENUDIDSHA1)
    }
    list
  }

  def getAnyOneUserId(row: Row): String = {
    row match {
      case v if StringUtils.isNotBlank(row.getAs[String](IMEI)) =>
        TM.concat(row.getAs[String](IMEI))
      case v if StringUtils.isNotBlank(row.getAs[String](MAC)) =>
        MC.concat(row.getAs[String](MAC))
      case v if StringUtils.isNotBlank(row.getAs[String](IDFA)) =>
        ID.concat(row.getAs[String](IDFA))
      case v if StringUtils.isNotBlank(row.getAs[String](OPENUDID)) =>
        OD.concat(row.getAs[String](OPENUDID))
      case v if StringUtils.isNotBlank(row.getAs[String](ANDROIDID)) =>
        AOD.concat(row.getAs[String](ANDROIDID))
      case v if StringUtils.isNotBlank(row.getAs[String](IMEIMD5)) =>
        TMM.concat(row.getAs[String](IMEIMD5))
      case v if StringUtils.isNotBlank(row.getAs[String](MACMD5)) =>
        MCM.concat(row.getAs[String](MACMD5))
      case v if StringUtils.isNotBlank(row.getAs[String](IDFAMD5)) =>
        IDM.concat(row.getAs[String](IDFAMD5))
      case v if StringUtils.isNotBlank(row.getAs[String](OPENUDIDMD5)) =>
        ODM.concat(row.getAs[String](OPENUDIDMD5))
      case v if StringUtils.isNotBlank(row.getAs[String](ANDROIDIDMD5)) =>
        AODM.concat(row.getAs[String](ANDROIDIDMD5))
      case v if StringUtils.isNotBlank(row.getAs[String](IMEISHA1)) =>
        TMS.concat(row.getAs[String](IMEISHA1))
      case v if StringUtils.isNotBlank(row.getAs[String](MACSHA1)) =>
        MCS.concat(row.getAs[String](MACSHA1))
      case v if StringUtils.isNotBlank(row.getAs[String](IDFASHA1)) =>
        IDS.concat(row.getAs[String](IDFASHA1))
      case v if StringUtils.isNotBlank(row.getAs[String](OPENUDIDSHA1)) =>
        ODS.concat(row.getAs[String](OPENUDIDSHA1))
      case v if StringUtils.isNotBlank(row.getAs[String](ANDROIDIDSHA1)) =>
        AODS.concat(row.getAs[String](ANDROIDIDSHA1))
    }
  }

}
