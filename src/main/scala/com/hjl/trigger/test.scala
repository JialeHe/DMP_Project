package com.hjl.trigger

import com.hjl.scheduler.JobComputing

/**
  *
  * @author jiale.he
  * @date 2019/05/30
  * @email jiale.he@mail.hypers.com
  */
object test {
  def main(args: Array[String]): Unit = {
    val name = "com.hjl.scheduler.report.terminal.EquipmentReport"
    val report: JobComputing = Class.forName(name).newInstance().asInstanceOf[JobComputing]
    report.process()
  }
}
