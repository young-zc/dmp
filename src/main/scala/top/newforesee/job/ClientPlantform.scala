package top.newforesee.job

import java.util
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql._
import top.newforesee.bean.{CityCountBean, NetworkType}
import top.newforesee.constants.Constant
import top.newforesee.dao.{INetworkImpl, NetWorkImpl}
import top.newforesee.job.base.Job
import top.newforesee.utils.{DBCPUtil, ResourcesUtils, Utils}

/**
  * xxx
  * creat by newforesee 2019/1/25
  */
object ClientPlantform extends Job {


  override def run(): Unit = {
    //    //初始化
    //    val spark: SparkSession = Utils.getSpark()
    //    val sc: SQLContext = spark.sqlContext
    //加载数据并注册临时表device,tmp_device
    loadData()
    //spark.sql("select * from device").show()

    //按照运营商:1,移动  2,联通  3,电信  4,未知
    val operator: DataFrame = spark.sql("select  *," +
      "(case when partInBidding=0 then 0 else successBidding/partInBidding end) biddingRate, " +
      "(case when show=0 then 0 else click/show end) clickRate " +
      "from (select " +
      "(case operator when 1 then '移动' when 2 then '联通' when 3 then '电信' when 4 then '未知'  end) operator, " +
      "sum(originalRequest) originalRequest, " +
      "sum(validRequest) validRequest, " +
      "sum(advRequest) advRequest, " +
      "sum(partInBidding) partInBidding, " +
      "sum(successBidding) successBidding, " +
      "sum(show) as show,sum(click) as click," +
      "sum(advCost) advCost,sum(advCharge) advCharge " +
      "from tmp_device group by operator)").cache()
    operator.show()


    //按照网络类型  1,2G  2,3G  3,wifi  4,未知  5,4G
    val networkType: DataFrame = spark.sql("select  *," +
      "(case when partInBidding=0 then 0 else successBidding/partInBidding end) biddingRate, " +
      "(case when show=0 then 0 else click/show end) clickRate " +
      "from (select " +

      "(case NetworkType when 1 then '2G' when 2 then '3G' when 3 then 'wifi' when 5 then '4G' else '未知' end) networkTypes, " +

      "sum(originalRequest) originalRequest, " +
      "sum(validRequest) validRequest, " +
      "sum(advRequest) advRequest, " +
      "sum(partInBidding) partInBidding, " +
      "sum(successBidding) successBidding, " +
      "sum(show) as show,sum(click) as click," +
      "sum(advCost) advCost,sum(advCharge) advCharge " +
      "from tmp_device group by networkTypes order by networkTypes)").cache()
    networkType.show()

    //按照设备类型 1,手机 2,平板
    val deviceType: DataFrame = spark.sql("select  *," +
      "(case when partInBidding=0 then 0 else successBidding/partInBidding end) biddingRate, " +
      "(case when show=0 then 0 else click/show end) clickRate " +
      "from (select " +

      "(case deviceType when 1 then '手机' when 2 then '平板'  else '未知' end) deviceType, " +

      "sum(originalRequest) originalRequest, " +
      "sum(validRequest) validRequest, " +
      "sum(advRequest) advRequest, " +
      "sum(partInBidding) partInBidding, " +
      "sum(successBidding) successBidding, " +
      "sum(show) as show,sum(click) as click," +
      "sum(advCost) advCost,sum(advCharge) advCharge " +
      "from tmp_device group by deviceType order by deviceType)").cache()
    deviceType.show()

    //1：android   2：ios   3：wp
    val platformType: DataFrame = spark.sql("select  *," +
      "(case when partInBidding=0 then 0 else successBidding/partInBidding end) biddingRate, " +
      "(case when show=0 then 0 else click/show end) clickRate " +
      "from (select " +

      "(case systemType when 1 then 'android' when 2 then 'ios'  else 'wp' end) systemType, " +

      "sum(originalRequest) originalRequest, " +
      "sum(validRequest) validRequest, " +
      "sum(advRequest) advRequest, " +
      "sum(partInBidding) partInBidding, " +
      "sum(successBidding) successBidding, " +
      "sum(show) as show,sum(click) as click," +
      "sum(advCost) advCost,sum(advCharge) advCharge " +
      "from tmp_device group by systemType order by systemType)").cache()

    platformType.show()


    val properties: Properties = DBCPUtil.getProperties

    def logoutSaving(format:String): Unit =logger.warn("INFO:: Saving >>>>> %s table to DB...".format(format))
    def logoutSaved(format:String): Unit =logger.warn("INFO:: table >>>>%s has SAVED to DB...".format(format))

    logoutSaving("networkType")
    Utils.saveAndSaveToDB(networkType, "networkType")
    logoutSaved("networkType")
    logoutSaving("operator")
    Utils.saveAndSaveToDB(operator, "operator")
    logoutSaved("operator")
    logoutSaving("deviceType")
    Utils.saveAndSaveToDB(deviceType, "deviceType")
    logoutSaved("deviceType")
    logoutSaving("platformType")
    Utils.saveAndSaveToDB(platformType, "platformType")
    logoutSaved("platformType")
  }


  /**
    * 保存文件并存入数据库
    *
    * @param rddCc 结果数据集
    */
  private def saveAndSaveToDB(rddCc: RDD[NetworkType]): Unit = {
    //rddCc.saveAsTextFile("/Users/newforesee/Intellij Project/DMP/src/main/resources/json")
    rddCc.foreachPartition((iter: Iterator[NetworkType]) => {
      if (iter.nonEmpty) {
        val dao: INetworkImpl = new NetWorkImpl
        val beans: util.LinkedList[NetworkType] = new util.LinkedList[NetworkType]
        iter.foreach((cc: NetworkType) => {
          beans.add(cc)
        })
        dao.saveToDB(beans)
      }
    })
  }

  /**
    * 加载数据并注册临时表
    *
    */
  def loadData(): Unit = {
    val ods: Dataset[Row] = spark.read.parquet(Constant.PATH + "/data_raw").coalesce(3)
    val rowRDD: RDD[Row] = ods.rdd.map((row: Row) => {
      //1,移动  2,联通  3,电信  4,未知
      //1,2G  2,3G  3,wifi  4,未知  5,4G
      //1：android   2：ios   3：wp
      val strings: Array[String] = row.toString().split(",")

      val operator: String = strings(26)
      val networkType: Int = strings(28).toInt
      val systemType: Int = strings(17).toInt
      val deviceType: Int = strings(34).toInt
      val requestmode: Int = strings(8).toInt
      val processnode: Int = strings(35).toInt
      val iseffective: Int = strings(30).toInt
      val isbilling: Int = strings(31).toInt
      val isbid: Int = strings(39).toInt
      val iswin: Int = strings(42).toInt
      val adorderid: Int = strings(2).toInt
      Row(operator, networkType, systemType, deviceType, requestmode, processnode, iseffective, isbilling, isbid, iswin, adorderid)
    })
    val schema: StructType = (new StructType)
      .add("operator", StringType, nullable = true)
      .add("NetworkType", IntegerType, nullable = true)
      .add("systemType", IntegerType, nullable = true)
      .add("deviceType", IntegerType, nullable = true)
      .add("requestmode", IntegerType, nullable = true)
      .add("processnode", IntegerType, nullable = true)
      .add("iseffective", IntegerType, nullable = true)
      .add("isbilling", IntegerType, nullable = true)
      .add("isbid", IntegerType, nullable = true)
      .add("iswin", IntegerType, nullable = true)
      .add("adorderid", IntegerType, nullable = true)
    val df: DataFrame = spark.createDataFrame(rowRDD, schema)
    df.createOrReplaceTempView("device")
    spark.sql("select operator, NetworkType, systemType, deviceType, " +
      "(case when requestmode=1 and processnode>=1 then 1 else 0 end) as originalRequest, " +
      "(case when requestmode=1 and processnode>=2 then 1 else 0 end) as validRequest, " +
      "(case when requestmode=1 and processnode>=3 then 1 else 0 end) as advRequest, " +
      "(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) partInBidding, " +
      "(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end) successBidding, " +
      "(case when requestmode=2 and iseffective=1 then 1 else 0 end) show, " +
      "(case when requestmode=3 and iseffective=1 then 1 else 0 end) click, " +
      "(case when iseffective=1 and isbilling=1 and iswin=1  then 1 else 0 end) advCost, " +
      "(case when iseffective=1 and isbilling=1 and iswin=1  then 1 else 0 end) advCharge " +
      "from device ").cache().createOrReplaceTempView("tmp_device")
  }


}
