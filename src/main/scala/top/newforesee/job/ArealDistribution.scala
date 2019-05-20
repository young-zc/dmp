package top.newforesee.job

import java.util
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SparkSession}
import top.newforesee.bean.CityCountBean
import top.newforesee.dao.ICityCount
import top.newforesee.dao.impl.CityCountImpl
import top.newforesee.job.base.Job
import top.newforesee.utils.{DBCPUtil, ResourcesUtils, Utils}

/**
  * 对城市信息进行统计相关指标
  * creat by newforesee 2019/1/21
  */
object ArealDistribution extends Job {
  override def run() = {
    //初始化
    val spark: SparkSession = Utils.getSpark()

    //对原始数据进行转化加载
    val ods_df: DataFrame = Utils.readData()
    //统计城市分布
    val rddCc: RDD[CityCountBean] = cityCount(ods_df.rdd)

    //创建临时表
    createTmpTable(spark, ods_df.rdd)

    val properties: Properties = DBCPUtil.getProperties

    val frame: DataFrame = spark.sql("select *, " +
      "(case when partInBidding=0 then 0 else successBidding/partInBidding end) biddingRate, " +
      "(case when show=0 then 0 else click/show end) clickRate " +
      "from areal_tmp ")
    ods_df.show()
    //frame.write.jdbc(properties.getProperty("url"),"tmp",properties)
    //frame.coalesce(1).write.csv("/Users/newforesee/Intellij Project/DMP/src/main/resources/data/csv")
    //保存到数据库
    saveAndSaveToDB(rddCc)
    //释放资源
    spark.close()
  }

  /**
    * 将原始数据提取字段构建临时表
    *
    * @param spark
    * @param odsRdd
    */
  private def createTmpTable(spark: SparkSession, odsRdd: RDD[Row]): Unit = {
    val rowRdd: RDD[Row] = odsRdd.map((r: Row) => {
      val strings: Array[String] = r.toString().split(",")
      val provinceName: String = strings(24)
      val cityName: String = strings(25)
      val requestmode: Int = strings(8).toInt
      val processnode: Int = strings(35).toInt
      val iseffective: Int = strings(30).toInt
      val isbilling: Int = strings(31).toInt
      val isbid: Int = strings(39).toInt
      val iswin: Int = strings(42).toInt
      val adorderid: Int = strings(2).toInt
      Row(provinceName, cityName, requestmode, processnode, iseffective, isbilling, isbid, iswin, adorderid)
    })


    val structType: StructType = (new StructType)
      .add("provinceName", StringType, nullable = true)
      .add("cityName", StringType, nullable = true)
      .add("requestmode", IntegerType, nullable = true)
      .add("processnode", IntegerType, nullable = true)
      .add("iseffective", IntegerType, nullable = true)
      .add("isbilling", IntegerType, nullable = true)
      .add("isbid", IntegerType, nullable = true)
      .add("iswin", IntegerType, nullable = true)
      .add("adorderid", IntegerType, nullable = true)

    val areal: DataFrame = spark.createDataFrame(rowRdd, structType)
    areal.createOrReplaceTempView("areal_ods")
    val areal_tmp: DataFrame =
      spark.sql(
        "select provinceName, cityName," +
          "sum((case when requestmode=1 and processnode>=1 then 1 else 0 end)) as originalRequest, " +
          "sum((case when requestmode=1 and processnode>=2 then 1 else 0 end)) as validRequest, " +
          "sum((case when requestmode=1 and processnode>=3 then 1 else 0 end)) as advRequest, " +
          "sum((case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end)) partInBidding, " +
          "sum((case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end)) successBidding, " +
          "sum((case when requestmode=2 and iseffective=1 then 1 else 0 end)) show, " +
          "sum((case when requestmode=3 and iseffective=1 then 1 else 0 end)) click, " +
          "sum((case when iseffective=1 and isbilling=1 and iswin=1  then 1 else 0 end)) advCost, " +
          "sum((case when iseffective=1 and isbilling=1 and iswin=1  then 1 else 0 end)) advCharge " +
          "from areal_ods " +
          "group by cityName,provinceName order by provinceName")
    areal_tmp.createOrReplaceTempView("areal_tmp")
  }

  /**
    * 保存文件并存入数据库
    *
    * @param rddCc 结果数据集
    */
  private def saveAndSaveToDB(rddCc: RDD[CityCountBean]): Unit = {
    rddCc.foreachPartition((iter: Iterator[CityCountBean]) => {
      if (iter.nonEmpty) {
        val dao: ICityCount = new CityCountImpl
        val beans: util.LinkedList[CityCountBean] = new util.LinkedList[CityCountBean]
        iter.foreach((cc: CityCountBean) => {
          beans.add(cc)
        })
        dao.saveToDB(beans)
      }
    })
  }

  /**
    * 统计省市数据量分布情况
    *
    * @param odsRdd 文件直接加载的数据集
    * @return
    */
  private def cityCount(odsRdd: RDD[Row]): RDD[CityCountBean] = {
    val tuples: RDD[(String, Int)] = odsRdd.map((row: Row) => {
      val strings: Array[String] = row.toString().split(",")
      (strings(24) + " " + strings(25), 1)
    })
    val reduced: RDD[(String, Int)] = tuples.reduceByKey((_: Int) + (_: Int))
    val rddCc: RDD[CityCountBean] = reduced.map((x: (String, Int)) => {
      val cc = new CityCountBean()
      cc.setCt(x._2)
      cc.setProvinceName(x._1.split(" ")(0))
      cc.setCityName(x._1.split(" ")(1))
      cc
    })
    rddCc
  }




}
