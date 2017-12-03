package com.zhiyou.bd18.sqoop.leadin

import java.util.Date

import com.zhiyou.bd18.common.DateUtil
import com.zhiyou.bd18.sqoop.SQClint

import org.apache.sqoop.model.{MFromConfig, MToConfig}

/**
  * @Author: HuiXin
  * @Description:  job
  * @Date: Created in 14:20 2017/12/2
  * @Monified By:
  */
object CompanyImport {
  //注意这个url要抽到一个代码里面,url改变的时候只需要改变那一个代码就好
  //把下面两句话移到创建的个类中去
  /*val url = "http://master:12000/sqoop/"
  val client = new SqoopClient(url)*/
  val client = SQClint.client
  //正常情况下,应该有个日期转换格式的工具类
  var  yyyymmdd =  DateUtil.convert2String(new Date(),"yyyyMMdd")
  //创建job   job中把Company的数据带入到hdf上
  //启动job
def createJob() = {
    //sqoop使用的sql中必须包含${CONDITIONS}
    //sqoop的lib目录下要把postgresql的驱动jar包拷贝进去
  val sql =
    """
      |select company_id
      |       ,company_address
      |       ,company_attr
      |       ,company_boss
      |       ,company_name
      |       ,company_phone
      |  from wsc.tb_company where
      |  ${CONDITIONS}
    """.stripMargin

    val job = client.createJob("btrip_pgdb","btrip_hdfs")
    val fromConfig = job.getFromJobConfig()
    val toConfig = job.getToJobConfig
    //打印两个job的配置信息
    showFromJobConfig(fromConfig)
    showToJobConfig(toConfig)
   // fromConfig.getStringInput("fromJobConfig.schemaName").setValue("wsc")
    fromConfig.getStringInput("fromJobConfig.sql").setValue(sql)
    //分区条件
    fromConfig.getStringInput("fromJobConfig.partitionColumn").setValue("company_id")

    toConfig.getEnumInput("toJobConfig.outputFormat").setValue("PARQUET_FILE")
    toConfig.getEnumInput("toJobConfig.compression").setValue("NONE")
    //导入到hdfs上目录的位置
    //根路径/sqoop/btrip_pg
    toConfig.getStringInput("toJobConfig.outputDirectory").setValue(s"/sqoop/btrip_pg/$yyyymmdd/tb_company")
    toConfig.getBooleanInput("toJobConfig.appendMode").setValue(true)
    job.setName(s"btrip_company_$yyyymmdd")
    val status = client.saveJob(job)
    if(status.canProceed){
      println("company job创建成功")
    }else{
      println(status)
      println("company job创建失败")
    }

  }
  //打印fromjob的配置信息项
  def showFromJobConfig(configs: MFromConfig) = {
    val configList = configs.getConfigs()
    for(i<- 0 until configList.size()){
      val config = configList.get(i)
      val inputs = config.getInputs
      for(j <- 0 until inputs.size()){
        val input = inputs.get(j)
        println(input)
      }
    }
  }
  //打印tojob的配置信息项
  def showToJobConfig(configs:MToConfig) = {
    val configList = configs.getConfigs
    for(i <- 0 until configList.size()){
      val config = configList.get(i)
      val inputs = config.getInputs
      for(j <- 0 until inputs.size()){
        val input = inputs.get(j)
        println(input)
      }
    }
  }
//删除job
  def deleteJob(name:String) = {
    try {
      client.deleteJob(name)
      println("删除成功")
    }catch {
      case e:Exception => e.printStackTrace()
    }
  }
  //启动job
  def startJob(name:String) = {
    client.startJob(name)
    client.getDriver
  }



  def main(args: Array[String]): Unit = {
    //为了让用户输入日期
    yyyymmdd = args match{
      case Array(date) => date
      case _ => yyyymmdd
    }
    //测试成功
   // createJob()
   // deleteJob(s"btrip_company_$yyyymmdd")
    startJob(s"btrip_company_$yyyymmdd")
  }
}
