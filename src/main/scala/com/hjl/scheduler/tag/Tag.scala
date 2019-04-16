package com.hjl.scheduler.tag

/**
  * 打标签接口
  * @author jiale.he
  * @date 2019/04/15
  * @email jiale.he@mail.hypers.com
  */
trait Tag {

  /**
    * 打标签方法
    * @param args
    * @return
    */
  def makeTags(args: Any*): List[(String, Int)]
}
