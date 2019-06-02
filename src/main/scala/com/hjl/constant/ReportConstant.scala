package com.hjl.constant

/**
  *
  * @author jiale.he
  *         date 2019/04/12
  *         email jiale.he@mail.hypers.com
  */
object ReportConstant {
  final val TEMP = "temp"
  final val LOCATION_SINK_TABLE_NAME = "location"
  final val OPERATOR_SINK_TABLE_NAME = "operator"
  final val NETWORK_SINK_TABLE_NAME = "network"
  final val EQUIPMENT_SINK_TABLE_NAME = "equipment"
  final val OS_SINK_TABLE_NAME = "os"
  final val MEDIA_SINK_TABLE_NAME = "media"

  final val PARQUET_SOURCE_PATH = "hdfs://mini04:9000/DMP/out"

  final val LOCATION_SINK_JSON_PATH = "hdfs://mini04:9000/DMP/reportout/location"
  final val OPERATOR_SINK_JSON_PATH = "hdfs://mini04:9000/DMP/reportout/operator"
  final val NETWORK_SINK_JSON_PATH = "hdfs://mini04:9000/DMP/reportout/network"
  final val EQUIPMENT_SINK_JSON_PATH = "hdfs://mini04:9000/DMP/reportout/equipment"
  final val OS_SINK_JSON_PATH = "hdfs://mini04:9000/DMP/reportout/os"
  final val MEDIA_SINK_JSON_PATH = "hdfs://mini04:9000/DMP/reportout/media"

  final val APP_ID = "appid"
  final val APP_NAME = "appname"

}
