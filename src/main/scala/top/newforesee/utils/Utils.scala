package top.newforesee.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import top.newforesee.constants.Constant

object Utils {
  /**
    * 准备sparkSession
    *
    * @return
    */
  def getSpark(): SparkSession = {
    //    val session: SparkSession = SparkSession.builder()
    //      .appName("DMP")
    //      .master("local[*]")
    //      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val builder: SparkSession.Builder = SparkSession.builder().config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //若是本地集群模式，需要单独设置
    if (ResourcesUtils.dMode.toString.toLowerCase().equals("local")) {
      builder.master("local[*]")
    }

    val spark: SparkSession = builder.getOrCreate()
    spark.sparkContext.setLogLevel("warn")

    spark
  }

  /**
    * 保存数据到数据库
    * @param df
    * @param tableName
    * @param mode
    */
  def saveAndSaveToDB(df:DataFrame,tableName:String,mode:String="overwrite"): Unit ={
    df.write.mode(mode).jdbc(DBCPUtil.getProperties.getProperty("url"),tableName,DBCPUtil.getProperties)
  }

  /**
    * 加载数据
    * @return
    */
  def readData(): DataFrame ={
    val spark: SparkSession = getSpark()
    spark.read.parquet(Constant.PATH+"/data_raw")
  }

}
