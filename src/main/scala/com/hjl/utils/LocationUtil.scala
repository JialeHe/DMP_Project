package com.hjl.utils

/**
  *
  * @author jiale.he
  * @date 2019/04/09
  * @email jiale.he@mail.hypers.com
  */
object LocationUtil {

  def adDspUtil(iseffective:Int, isbilling:Int, isbid:Int,
                iswin:Int, adorderid:Int, winprice:Double, adpayment:Double) = {
    if (iseffective==1 && isbilling==1 && isbid==1){
      List[Double](1,0,0,0)
    }else if(iseffective==1 && isbilling==1 && iswin==1){
      if(adorderid != 0){
        List[Double](0,1,winprice/1000,adpayment/1000)
      }else{
        List[Double](0,0,winprice/1000,adpayment/1000)
      }
    }else{
      List[Double](0,0,0,0)
    }
  }

  /**
    * 点击， 展示
    * @param requestmode
    * @param iseffective
    * @return
    */
  def showClickUtil(requestmode:Int, iseffective:Int):List[Double] = {
    if(requestmode == 2 && iseffective == 1){
      List[Double](1,0)
    }else if(requestmode == 3 && iseffective == 1){
      List[Double](0,1)
    }else {
      List[Double](0,0)
    }
  }

  /**
    * 原始请求数， 有效请求数
    * @param requestmode
    * @param processnode
    * @return
    */
  def requestUtil(requestmode:Int, processnode:Int): List[Double] = {
    if (requestmode==1 && processnode==1){
      List[Double](1,0,0)
    }else if(requestmode==1 && processnode==2){
      List[Double](1,1,0)
    }else if(requestmode==1 && processnode==3){
      List[Double](1,1,1)
    }else{
      List[Double](0,0,0)
    }
  }

}
