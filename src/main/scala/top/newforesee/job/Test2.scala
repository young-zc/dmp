package top.newforesee.job

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * xxx
  * creat by newforesee 2019-01-29
  */
object Test2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("wholeTextFiles").setMaster("local[*]")
    val context = new SparkContext(conf)
    val rdd: RDD[String] = context.textFile("D:\\Idea project\\dmp\\src\\main\\resources\\score.txt")
    val rddtuple: RDD[(String, Int)] = rdd.map((x: String) => {
      val strings: Array[String] = x.split(" ")
      (strings(0), strings(1).toInt)
    })
    val grouped: RDD[(String, Iterable[Int])] = rddtuple.groupByKey()
    val classScore: RDD[(String, Array[Int])] = grouped.map((x: (String, Iterable[Int])) => {
      val classed: String = x._1
      val score: Array[Int] = x._2.toArray.sortWith(_ > _).take(3)
      (classed, score)
    })
    classScore.foreach(t => {
      println(t._1)
      t._2.foreach(println)
      println("----------------------")
    })
    context.stop()



  }

}
