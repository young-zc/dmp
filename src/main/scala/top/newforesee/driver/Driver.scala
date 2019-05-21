package top.newforesee.driver

import org.apache.log4j.{Level, Logger}
import top.newforesee.job.base.Job

object Driver {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    ApplicationProperties.parserArgs(args)

    logger.warn("INFO :: ApplicationProperties=>%s".format(ApplicationProperties.toString))

    logger.info("INFO :: Driver Running , call [%s]".format(ApplicationProperties.PACKAGENAMES.toString()))
    _start()
  }
  var className=""
  private def _start(): Unit = try {

    ApplicationProperties.PACKAGENAMES.foreach((cn: String) =>{
      className=cn
      Class.forName("%s$".format(cn))
        .getField("MODULE$")
        .get(classOf[Job])
        .asInstanceOf[Job].main(null)
    })


  } catch {
    case e: Exception =>
      logger.log(Level.ERROR, " (%s) job failed..Error message is - (%s)".format(className, e.getMessage))
      throw e
    case e: Throwable =>
      logger.log(Level.ERROR, " (%s) job failed..Error message is - (%s)".format(className, e.getMessage))
      throw e
    case e: NullPointerException =>
      logger.log(Level.ERROR, " (%s) job failed..Error message is - (%s)".format(className, e.getMessage))
      throw e
  }

}
