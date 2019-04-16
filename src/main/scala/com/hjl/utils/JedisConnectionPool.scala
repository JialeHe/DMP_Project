package com.hjl.utils

import com.hjl.constant.CommonConstant
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * jedis连接池
  *
  * @author jiale.he
  * @date 2019/04/12
  * @email jiale.he@mail.hypers.com
  */
object JedisConnectionPool {
  // 获取配置对象
  val conf = new JedisPoolConfig()
  // 设置最大连接数
  conf.setMaxTotal(40)
  // 设置最大空闲连接数
  conf.setMaxIdle(20)
  // 创建jedis连接池
  val pool = new JedisPool(conf, CommonConstant.REDIS_HOST, CommonConstant.REDIS_PORT, 10000)

  def getConnect: Jedis = {
    pool.getResource
  }
}
