package com.hjl.trigger

import com.hjl.scheduler.JobComputing
import org.slf4j.{Logger, LoggerFactory}

/**
  *
  * @author jiale.he
  * @date 2019/05/30
  * @email jiale.he@mail.hypers.com
  */
object TaskDriver {

  private val logger: Logger = LoggerFactory.getLogger(TaskDriver.getClass)

  val PARQUET = "com.hjl.scheduler.parquet.Bz2Parquet"
  val REPORT = "com.hjl.scheduler.report."
  val TAG = "com.hjl.scheduler.tag."

  def getClassList(): List[String] = {
    var classNames: List[String] = List[String]()

    classNames :+= PARQUET
    classNames :+= REPORT.concat("App2Redis")
    classNames :+= REPORT.concat("terminal.EquipmentReport")
    classNames :+= REPORT.concat("terminal.NetWorkReport")
    classNames :+= REPORT.concat("terminal.OperatorReport")
    classNames :+= REPORT.concat("terminal.OSReport")
    classNames :+= REPORT.concat("media.MediaReport2")
    classNames :+= REPORT.concat("location.LocationReport")
//    classNames :+= TAG.concat("Baidu2Redis")
    classNames :+= TAG.concat("TagContext")
    classNames
  }


  def main(args: Array[String]): Unit = {

    val classNames: List[String] = getClassList()
    for (elem <- classNames) {
      val job: JobComputing = Class.forName(elem).newInstance().asInstanceOf[JobComputing]
      println("Start " + job.getClass.getName + "Job")
      job.process()
      println("End " + job.getClass.getName + "Job")
      Thread.sleep(1500)
    }
    logger.info("Successful Project Execution")
  }
}
