package com.imooc.flink.course04

import scala.util.Random

/**
 * @ClassName: DBUtils
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/8 4:35 下午
 */

object DBUtils {

  def getConnection() = {
    new Random().nextInt(10) + ""
  }

  def returnConnection(connection: String) = {

  }
}
