package com.hjl.scheduler.parquet

import com.hjl.scheduler.JobComputing
import com.hjl.utils.{SchemaUtil, TypeConvertUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}


/**
  * 处理数据文件 将其转化成parquet格式
  * 1. 将数据转化成Parquet格式
  * 2. 采用KryoSerializer方式
  * 3. Parquet文件采用Snappy压缩
  *
  * @author jiale.he
  * @date 2019/04/09
  * @email jiale.he@mail.hypers.com
  */
object Bz2Parquet extends JobComputing{
  def main(args: Array[String]): Unit = {

    // 检测参数,接受参数
    checkParam(args, 2)
    val Array(inputPath,outputPath) = args
    // 初始化环境
    initAll(this.getClass.getName, true)

    val sourceRDD: RDD[String] = sc.textFile(inputPath)


    val rowRDD: RDD[Row] = sourceRDD.map(line => line.split(",", -1))
      .filter(data => data.length >= 85)
      .map(data => Row(
        TypeConvertUtil.s2s(data(0)), TypeConvertUtil.s2I(data(1)), TypeConvertUtil.s2I(data(2)),
        TypeConvertUtil.s2I(data(3)), TypeConvertUtil.s2I(data(4)), TypeConvertUtil.s2s(data(5)),
        TypeConvertUtil.s2s(data(6)), TypeConvertUtil.s2I(data(7)), TypeConvertUtil.s2I(data(8)),
        TypeConvertUtil.s2D(data(9)), TypeConvertUtil.s2D(data(10)), TypeConvertUtil.s2s(data(11)),
        TypeConvertUtil.s2s(data(12)), TypeConvertUtil.s2s(data(13)), TypeConvertUtil.s2s(data(14)),
        TypeConvertUtil.s2s(data(15)), TypeConvertUtil.s2s(data(16)), TypeConvertUtil.s2I(data(17)),
        TypeConvertUtil.s2s(data(18)), TypeConvertUtil.s2s(data(19)), TypeConvertUtil.s2I(data(20)),
        TypeConvertUtil.s2I(data(21)), TypeConvertUtil.s2s(data(22)), TypeConvertUtil.s2s(data(23)),
        TypeConvertUtil.s2s(data(24)), TypeConvertUtil.s2s(data(25)), TypeConvertUtil.s2I(data(26)),
        TypeConvertUtil.s2s(data(27)), TypeConvertUtil.s2I(data(28)), TypeConvertUtil.s2s(data(29)),
        TypeConvertUtil.s2I(data(30)), TypeConvertUtil.s2I(data(31)), TypeConvertUtil.s2I(data(32)),
        TypeConvertUtil.s2s(data(33)), TypeConvertUtil.s2I(data(34)), TypeConvertUtil.s2I(data(35)),
        TypeConvertUtil.s2I(data(36)), TypeConvertUtil.s2s(data(37)), TypeConvertUtil.s2I(data(38)),
        TypeConvertUtil.s2I(data(39)), TypeConvertUtil.s2D(data(40)), TypeConvertUtil.s2D(data(41)),
        TypeConvertUtil.s2I(data(42)), TypeConvertUtil.s2s(data(43)), TypeConvertUtil.s2D(data(44)),
        TypeConvertUtil.s2D(data(45)), TypeConvertUtil.s2s(data(46)), TypeConvertUtil.s2s(data(47)),
        TypeConvertUtil.s2s(data(48)), TypeConvertUtil.s2s(data(49)), TypeConvertUtil.s2s(data(50)),
        TypeConvertUtil.s2s(data(51)), TypeConvertUtil.s2s(data(52)), TypeConvertUtil.s2s(data(53)),
        TypeConvertUtil.s2s(data(54)), TypeConvertUtil.s2s(data(55)), TypeConvertUtil.s2s(data(56)),
        TypeConvertUtil.s2I(data(57)), TypeConvertUtil.s2D(data(58)), TypeConvertUtil.s2I(data(59)),
        TypeConvertUtil.s2I(data(60)), TypeConvertUtil.s2s(data(61)), TypeConvertUtil.s2s(data(62)),
        TypeConvertUtil.s2s(data(63)), TypeConvertUtil.s2s(data(64)), TypeConvertUtil.s2s(data(65)),
        TypeConvertUtil.s2s(data(66)), TypeConvertUtil.s2s(data(67)), TypeConvertUtil.s2s(data(68)),
        TypeConvertUtil.s2s(data(69)), TypeConvertUtil.s2s(data(70)), TypeConvertUtil.s2s(data(71)),
        TypeConvertUtil.s2s(data(72)), TypeConvertUtil.s2I(data(73)), TypeConvertUtil.s2D(data(74)),
        TypeConvertUtil.s2D(data(75)), TypeConvertUtil.s2D(data(76)), TypeConvertUtil.s2D(data(77)),
        TypeConvertUtil.s2D(data(78)), TypeConvertUtil.s2s(data(79)), TypeConvertUtil.s2s(data(80)),
        TypeConvertUtil.s2s(data(81)), TypeConvertUtil.s2s(data(82)), TypeConvertUtil.s2s(data(83)),
        TypeConvertUtil.s2I(data(84))
      ))
    val df: DataFrame = sqlContext.createDataFrame(rowRDD, SchemaUtil.schema)

    df.repartition(1).write.parquet(outputPath)

    stopSparkContext()
  }
}
