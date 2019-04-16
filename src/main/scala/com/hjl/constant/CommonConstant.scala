package com.hjl.constant

/**
  *
  * @author jiale.he
  * @date 2019/04/12
  * @email jiale.he@mail.hypers.com
  */
object CommonConstant {

  final val PARQUET_SOURCE_PATH = "hdfs://mini04:9000/DMP/out"
  final val APPDIR_SOURCE_PATH = "hdfs://mini04:9000/DMP/in/app_dict.txt"
  final val STOP_WORD_SOURCE_PATH = "hdfs://mini04:9000/DMP/in/stopwords.txt"

  final val REDIS_HOST = "mini04"
  final val REDIS_PORT = 6379
}
