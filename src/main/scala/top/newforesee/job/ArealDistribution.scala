package top.newforesee.job

import java.util
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SparkSession}
import top.newforesee.bean.CityCountBean
import top.newforesee.dao.ICityCount
import top.newforesee.dao.impl.CityCountImpl
import top.newforesee.utils.{DBCPUtil, ResourcesUtils}

/**
  * 对城市信息进行统计相关指标
  * creat by newforesee 2019/1/21
  */
object ArealDistribution {
  def main(args: Array[String]): Unit = {
    //初始化
    val session: SparkSession = firestOfAll()
    val ssc: SQLContext = session.sqlContext
    //对原始数据进行转化加载
    val odsRdd: RDD[Row] = transfome(session)
    //统计城市分布
    val rddCc: RDD[CityCountBean] = cityCount(odsRdd)

    //创建临时表
    createTmpTable(ssc, session, odsRdd)

    val properties: Properties = DBCPUtil.getProperties

    val frame: DataFrame = ssc.sql("select *, " +
      "(case when partInBidding=0 then 0 else successBidding/partInBidding end) biddingRate, " +
      "(case when show=0 then 0 else click/show end) clickRate " +
      "from areal_tmp ")
    frame.show()
    //frame.write.jdbc(properties.getProperty("url"),"tmp",properties)
    //frame.coalesce(1).write.csv("/Users/newforesee/Intellij Project/DMP/src/main/resources/dmp/csv")
    //保存到数据库
    //saveAndSaveToDB(rddCc)
    //释放资源
    session.close()
  }

  /**
    * 将原始数据提取字段构建临时表
    *
    * @param session
    * @param odsRdd
    */
  private def createTmpTable(ssc: SQLContext, session: SparkSession, odsRdd: RDD[Row]): Unit = {
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

    val areal: DataFrame = ssc.createDataFrame(rowRdd, structType)
    areal.createOrReplaceTempView("areal_ods")
    val areal_tmp: DataFrame =
      ssc.sql(
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
    rddCc.saveAsTextFile("/Users/newforesee/Intellij Project/DMP/src/main/resources/json")
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

  /**
    * 将文件转化为parquet格式并加载
    *
    * @param session SparkSession
    * @return
    */
  private def transfome(session: SparkSession): RDD[Row] = {
    //    val ds: Dataset[String] = session.read.textFile("/Users/newforesee/Intellij Project/DMP/src/main/resources/2016-10-01_06_p1_invalid.1475274123982.log")
    //    ds.toDF().write.parquet("/Users/newforesee/Intellij Project/DMP/src/main/resources/dmp")
    //    val ds: Dataset[String] = session.read.textFile("/Users/newforesee/Intellij Project/DMP/src/main/resources/2016-10-01_06_p1_invalid.1475274123982.log.FINISH")
    //    ds.toDF().write.parquet("/Users/newforesee/Intellij Project/DMP/src/main/resources/dmp1")

    val df: DataFrame = session.read.parquet("/Users/newforesee/Intellij Project/DMP/src/main/resources/dmp1")
    val odsRdd: RDD[Row] = df.rdd.cache()
    odsRdd
  }

  /**
    * 准备sparkSession
    *
    * @return
    */
  private def firestOfAll(): SparkSession = {
    //    val session: SparkSession = SparkSession.builder()
    //      .appName("DMP")
    //      .master("local[*]")
    //      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val builder: SparkSession.Builder = SparkSession.builder().config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").appName(ArealDistribution.getClass.getSimpleName)

    //若是本地集群模式，需要单独设置
    if (ResourcesUtils.dMode.toString.toLowerCase().equals("local")) {
      builder.master("local[*]")
    }

    val spark: SparkSession = builder.getOrCreate()

    spark
  }
}
