package com.zhiyou.bd18.sqoop.link

import org.apache.sqoop.client.SqoopClient
import java.util

import org.apache.sqoop.model.{MConfig, MInput}
/**
  * @Author: HuiXin
  * @Description: link  一个项目只创建一个link就可以了
  * @Date: Created in 14:28 2017/12/2
  * @Monified By:
  */
object LinkCreator {
  //创建hdfslink   为了项目可迁移来写,有时还可以在集群里面去写
  //创建 postgresql link jdbc link
  /*link是连接两边的通道,中间有一个job,注意数据的来源和和数据的去向,
  这里数据来源是'大象sql',数据去向是'hdfs'
  link的类型根据两边数据类型的不同而不同
  */
  //报错信息404,原因url后面没有加上"/"
  val url = "http://master:12000/sqoop/"
  val client = new SqoopClient(url)
  def createHdfsLink() = {
    val hdfsLink = client.createLink("hdfs-connector")
    val linkConfig = hdfsLink.getConnectorLinkConfig()
    //打印配置参数,测试的时候使用
    //printLinkConfiguration(linkConfig.getConfigs)
    //错误分析:把seyValue写错了
    linkConfig.getStringInput("linkConfig.uri").setValue("hdfs://master:9000")
    linkConfig.getStringInput("linkConfig.confDir").setValue("/opt/SoftWare/Hadoop/hadoop-2.7.3/etc/hadoop")
    //linkConfig.getMapInput("linkConfig.configOverrides").setValue()
    hdfsLink.setName("btrip_hdfs")
    val status = client.saveLink(hdfsLink)
    if (status.canProceed) {
      println("hdfs-link创建成功")
    } else {
      println("hdfs-link创建失败")
    }
  }

  def createPostgresqlLink() = {
      val pglink = client.createLink("generic-jdbc-connector")
    val linkConfig = pglink.getConnectorLinkConfig
    //打印参数配置
    //printLinkConfiguration(linkConfig.getConfigs)
    linkConfig.getStringInput("linkConfig.jdbcDriver").setValue("org.postgresql.Driver")
    linkConfig.getStringInput("linkConfig.connectionString").setValue("jdbc:postgresql://192.168.6.36:5432/WscHMS")
    linkConfig.getStringInput("linkConfig.username").setValue("postgres")
    linkConfig.getStringInput("linkConfig.password").setValue("root")
    linkConfig.getStringInput("dialect.identifierEnclose").setValue(" ")
    pglink.setName("btrip_pgdb")
    val status = client.saveLink(pglink)
    if(status.canProceed){
      println("postgresql_link创建成功")
    }else{
      println("postgresql_link创建失败")
    }
  }
  //打印
  def printLinkConfiguration(configs: util.List[MConfig]) = {
    for (i <- 0 until configs.size()) {
      val inputs: java.util.List[MInput[_]] = configs.get(i).getInputs
      for (j <- 0 until inputs.size()) {
        val input = inputs.get(j)
        println(input)
      }
    }
  }

  //删除link
  def deleteLink(name:String) = {
    try{
      client.deleteLink(name)
      println("删除成功")
    }catch{
      case e:Exception => print("不存在")
    }
  }


  def main(args: Array[String]): Unit = {
      //createHdfsLink()
    deleteLink("btrip_hdfs")
    //-------上面测试成功----------
    //createPostgresqlLink()
   deleteLink("btrip_pgdb")
  }
}
