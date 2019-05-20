package top.newforesee.job

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import top.newforesee.constants.Constant
import top.newforesee.job.base.Job

object TransEtlJob extends Job {
  override def run(): Unit = {

    val ds: Dataset[String] = spark.read.textFile(Constant.PATH + "/data")
    ds.toDF().write.mode("overwrite").parquet(Constant.PATH + "/data_raw")

  }

}
