package com.hjl.scheduler.tag

import java.text.SimpleDateFormat
import java.util.Date

import com.hjl.constant.{CommonConstant, TagConstant}
import com.hjl.scheduler.JobComputing
import com.hjl.scheduler.tag.tagcomputing._
import com.hjl.utils.{JedisConnectionPool, TagUtil}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.Jedis

/**
  * 合并标签
  * 图计算
  * 统一用户标识
  *
  * @author jiale.he
  * @date 2019/04/15
  * @email jiale.he@mail.hypers.com
  */
class TagContext extends JobComputing {

  private val logger: Logger = LoggerFactory.getLogger(classOf[TagContext])

  override def process(): Unit = {
    initAll(this.getClass.getName, flag = true)

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val day: String = format.format(new Date())

    // HBase连接
    // 配置连接
    val load: Config = ConfigFactory.load()
    val hbaseTableName = load.getString(TagConstant.HB_TABLE_NAME)
    val hbConfig = sc.hadoopConfiguration
    hbConfig.set(TagConstant.HB_ZK_KEY, load.getString(TagConstant.HB_ZK_VALUE))
    // 加载连接
    val hbConn = ConnectionFactory.createConnection(hbConfig)
    val admin: Admin = hbConn.getAdmin
    // 判断表是否 存在
    if (!admin.tableExists(TableName.valueOf(hbaseTableName))) {
      logger.info("Table Being Created!")
      // 创建表描述器
      val tableDescript = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      // 创建列族
      val columnDescriptor = new HColumnDescriptor(TagConstant.HB_COLUNM_FAMILY)
      // 列族加入表
      tableDescript.addFamily(columnDescriptor)
      admin.createTable(tableDescript)
      admin.close()
      hbConn.close()
    }
    // 创建一个JobConf任务
    val jobConf = new JobConf(hbConfig)
    // 指定输出Key类型
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    // 指定输出到哪个表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)
    logger.info("Table Already Created!")

    // 获取appdir广播
    val appdirMap: Map[String, String] = sc.textFile(CommonConstant.APPDIR_SOURCE_PATH)
      .map(_.split("\t", -1))
      .filter(_.length >= 5)
      .map(arr => (arr(4), arr(1)))
      .collect
      .toMap
    // 广播app字典文件
    val appBroacast: Broadcast[Map[String, String]] = sc.broadcast(appdirMap)

    // 获取StopWords
    val stopwords: Map[String, Int] = sc.textFile(CommonConstant.STOP_WORD_SOURCE_PATH)
      .map((_, 0))
      .collect
      .toMap
    val stopWordsBroadcast: Broadcast[Map[String, Int]] = sc.broadcast(stopwords)

    val sourceDF: DataFrame = sqlContext.read.parquet(CommonConstant.PARQUET_SOURCE_PATH)

    // (UserIDHash， list(标签名，值) )
    val baseRDD = sourceDF.map(row => {
      // 用户ID
      // List(AOD: cd6e403047b20ff, TMM: eed790fade337966254d216059a17084)
      val list = TagUtil.getAnyAllUserId(row)
      (list, row)
    })

    val nodeRDD: RDD[(Long, List[(String, Int)])] = baseRDD.flatMap(tp => {
      val row = tp._2
      val userIds = tp._1
      val jedis: Jedis = JedisConnectionPool.getConnect
      // 广告位标签 List((LC 9,1), (LN 视频暂停悬浮,1))
      val adTag: List[(String, Int)] = AdTag.makeTags(row)
      // appName标签 List((APP 爱奇艺,1))
      val appTag: List[(String, Int)] = AppTag.makeTags(row, appBroacast)
      // 渠道标签 List((CN 100018,1))
      val adplatTag: List[(String, Int)] = AdplatformproviderTag.makeTags(row)
      // 设备标签 List((Android D00010001,1), (2G D00020004,1), (电信 D00030003,1))
      val deviceTag: List[(String, Int)] = DeviceTag.makeTags(row)
      // 关键字标签 List((K 言情剧,1), (K 内地剧场,1), (K 家庭剧,1))
      val stopwordsTag: List[(String, Int)] = KeyWordsTag.makeTags(row, stopWordsBroadcast)
      // 地域标签
      val locationTag: List[(String, Int)] = LocationTag.makeTags(row)
      // 商圈标签
      val businessTag: List[(String, Int)] = BusinessTag.makeTags(row, jedis)

      //将所有标签放到一起
      //(LN 插屏,1), (APP 其他,1), (CN 100002,1), (Android D00010001,1), (其他 D00020005,1), (其他 D00030004,1), (ZP 湖南省,1), (ZC 益阳市,1))
      val RowTag: List[(String, Int)] = adTag ++ appTag ++ adplatTag ++ deviceTag ++ stopwordsTag ++ locationTag ++ businessTag
      jedis.close()

      val resultList: List[(String, Int)] = userIds.map((_, 0)) ++ RowTag

      userIds.map(uId => {
        if (userIds.head.equals(uId)) {
          (uId.hashCode.toLong, resultList)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      })
    }).distinct()
      .filter(_._2.nonEmpty)


    // 构造边
    val edge: RDD[Edge[Int]] = baseRDD.flatMap(tp => {
      val userIds: List[String] = tp._1
      userIds.map(uId =>
        Edge(userIds.head.hashCode, uId.hashCode.toLong, 0)
      )
    })

    // 构建图
    val graph: Graph[List[(String, Int)], Int] = Graph(nodeRDD, edge)
    /**
      * 连通图最小节点
      * (-1318020447,-1318020447)
      * (2056084107,-1112180386)
      * (377027582,377027582)
      */
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

    /**
      * userIdHashCode,UserTags
      * (-619274711,(-1551057431,List()))
      * (1268289187,(-719478584,List()))
      * (562999815,(362212046,List((AOD: 8e8edf6b30afcb57,0), (TMM: f583d186f874971d05d4be6f514ad035,0), (LC 9,1), (LN 视频暂停悬浮,1), (APP 爱奇艺,1), (CN 100018,1), (Android D00010001,1), (其他 D00020005,1))))
      */
    val verticesJoined: RDD[(VertexId, (VertexId, List[(String, Int)]))] = vertices.join(nodeRDD)


    verticesJoined.map {
      // 将第二Id过滤，只保留UserID和TagsList
      case (uId, (commonId, tagList)) => (uId, tagList)
    }.reduceByKey {
      case (list1, list2) => (list1 ++ list2)
        // UserId,list(String,sumInt)
        .groupBy(_._1).mapValues(_.map(_._2).sum).toList
    }.map {
      case (uId, tagList) => {
        val put = new Put(Bytes.toBytes(uId))
        val tag: String = tagList.map(data => data._1 + "," + data._2).mkString(";")
        // 将数据输入到Hbase
        put.addImmutable(Bytes.toBytes(TagConstant.HB_COLUNM_FAMILY), Bytes.toBytes(day), Bytes.toBytes(tag))
        (new ImmutableBytesWritable(), put)
      }
    }.saveAsHadoopDataset(jobConf)

    stopSparkContext()
  }
}
