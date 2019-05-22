package top.newforesee.driver

import org.apache.log4j.{Level, Logger}
import top.newforesee.job.base.Job

object Driver {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    ApplicationProperties.parserArgs(args)

    logger.warn("INFO :: ApplicationProperties=>%s".format(ApplicationProperties.toString))

    _start()
  }

  var className = ""

  private def _start(): Unit = try {
    logger.warn("INFO :: Start check classes")
    for(cn<-ApplicationProperties.PACKAGENAMES){
      className = cn
      logger.warn("INFO :: Driver Running , call [%s]".format(className))
      Class.forName("%s$".format(cn))
        .getField("MODULE$")
        .get(classOf[Job])
        .asInstanceOf[Job].main(null)
    }


    //    ApplicationProperties.PACKAGENAMES.foreach((cn: String) =>{
    //      className=cn
    //      logger.info("INFO :: Driver Running , call [%s]".format(className))
    //      Class.forName("%s$".format(cn))
    //        .getField("MODULE$")
    //        .get(classOf[Job])
    //        .asInstanceOf[Job].main(null)
    //    })


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
  private def _submiter(cn:String){

  }

}
