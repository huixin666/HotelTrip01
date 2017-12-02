package com.zhiyou.bd18.sqoop.leadin

import org.apache.sqoop.client.SqoopClient
import org.apache.sqoop.model.{MFromConfig, MToConfig}

/**
  * @Author: HuiXin
  * @Description:  job
  * @Date: Created in 14:20 2017/12/2
  * @Monified By:
  */
object CompanyImport {
  val url = "http://master:12000/sqoop/"
  val client = new SqoopClient(url)
  //创建job   job中把Company的数据带入到hdf上
  //启动job
def createJob() = {
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
    toConfig.getStringInput("toJobConfig.outputDirectory").setValue("/sqoop/btrip_pg")
    toConfig.getBooleanInput("toJobConfig.appendMode").setValue(true)
    job.setName("btrip_company")
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
    //测试成功
    createJob()
    //deleteJob("btrip_company")
  }
}
