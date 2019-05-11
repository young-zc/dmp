package top.newforesee.job

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * xxx
  * creat by newforesee 2019-01-26
  */
object Test {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("DMP")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
    val frame: DataFrame = session.read.parquet("/Users/newforesee/Intellij Project/DMP/src/main/resources/dmp1")
    val unit: RDD[(String, String, String, String)] = frame.rdd.map((row: Row) => {
      val strings: Array[String] = row.toString().split(",")
      (strings(14), strings(79), strings(80), strings(83))
    })
    unit.foreach((row: (String, String, String, String)) =>{
     //if (row._2 != "" )println(row._2)
      println(row)
    })


  }

}
