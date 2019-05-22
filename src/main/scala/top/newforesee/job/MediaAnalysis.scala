package top.newforesee.job

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql._
import top.newforesee.constants.Constant
import top.newforesee.job.base.Job
import top.newforesee.utils.ResourcesUtils

/**
  * 媒体相关分析,渠道报表
  * creat by newforesee 2019-02-02
  */
object MediaAnalysis extends Job{
  override def run(): Unit = {

    createTmpTable()

  }


  /**
    * 将原始数据提取字段构建临时表
    */
  private def createTmpTable(): Unit = {
    val ods: Dataset[Row] = spark.read.parquet(Constant.PATH+"/data_raw").coalesce(3)

    val rowRdd: RDD[Row] = ods.rdd.map((r: Row) => {
      val strings: Array[String] = r.toString().split(",")
      val applicationName: String = strings(14)
      //val cityName: String = strings(25)
      val requestmode: Int = strings(8).toInt
      val processnode: Int = strings(35).toInt
      val iseffective: Int = strings(30).toInt
      val isbilling: Int = strings(31).toInt
      val isbid: Int = strings(39).toInt
      val iswin: Int = strings(42).toInt
      val adorderid: Int = strings(2).toInt
      Row(applicationName, requestmode, processnode, iseffective, isbilling, isbid, iswin, adorderid)
    })


    val structType: StructType = (new StructType)
      .add("applicationName", StringType, nullable = true)
      .add("requestmode", IntegerType, nullable = true)
      .add("processnode", IntegerType, nullable = true)
      .add("iseffective", IntegerType, nullable = true)
      .add("isbilling", IntegerType, nullable = true)
      .add("isbid", IntegerType, nullable = true)
      .add("iswin", IntegerType, nullable = true)
      .add("adorderid", IntegerType, nullable = true)

    val areal: DataFrame = spark.createDataFrame(rowRdd, structType)
    areal.createOrReplaceTempView("app_ods")
    val app_tmp: DataFrame =
      spark.sql(
        "select applicationName," +
          "sum((case when requestmode=1 and processnode>=1 then 1 else 0 end)) as originalRequest, " +
          "sum((case when requestmode=1 and processnode>=2 then 1 else 0 end)) as validRequest, " +
          "sum((case when requestmode=1 and processnode>=3 then 1 else 0 end)) as advRequest, " +
          "sum((case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end)) partInBidding, " +
          "sum((case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end)) successBidding, " +
          "sum((case when requestmode=2 and iseffective=1 then 1 else 0 end)) show, " +
          "sum((case when requestmode=3 and iseffective=1 then 1 else 0 end)) click, " +
          "sum((case when iseffective=1 and isbilling=1 and iswin=1  then 1 else 0 end)) advCost, " +
          "sum((case when iseffective=1 and isbilling=1 and iswin=1  then 1 else 0 end)) advCharge " +
          "from app_ods " +
          "group by applicationName")
    app_tmp.createOrReplaceTempView("app_tmp")
    app_tmp.show(50)
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
