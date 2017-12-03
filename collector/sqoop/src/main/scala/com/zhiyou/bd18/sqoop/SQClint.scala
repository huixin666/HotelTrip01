package com.zhiyou.bd18.sqoop

import org.apache.sqoop.client.SqoopClient

/**
  * @Author: HuiXin
  * @Description:
  * @Date: Created in 23:41 2017/12/2
  * @Monified By:
  */
object SQClint {
  val url = "http://master:12000/sqoop/"
  val client = new SqoopClient(url)

}
