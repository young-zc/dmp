package top.newforesee.job.base

import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import top.newforesee.utils.Utils

trait Job {
  val logger: Logger = Logger.getLogger(this.getClass)

  class ExecutorRunningException(s: String) extends Exception(s) {}

  val hmErrors: util.Map[String, String] = new util.HashMap[String, String]()
  hmErrors.put("E001", "ERROR :: E001 - Job Running Exception - %s")
  //hmErrors.put("E002", "ERROR :: E002 - Oozie Running Exception - %s")
  val spark: SparkSession = Utils.getSpark()
  spark.sparkContext.setLogLevel("warn")

  def getError(sCode: String, sBuild: String): String = hmErrors.get(sCode).format(sBuild)

  def main(args: Array[String]): Unit = {
    val start: Long = System.currentTimeMillis()

    logger.log(Level.WARN, s"Starting Job.")
    // Try catch here
    run()
    logger.log(Level.WARN, s"Finished Job in ${(System.currentTimeMillis() - start) / 1000} seconds.")
  }

  def run()

}
