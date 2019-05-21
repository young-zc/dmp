package top.newforesee.driver

import java.util

import org.apache.log4j.Logger
import top.newforesee.constants.{Constant, Constants}

object ApplicationProperties {
  private val logger: Logger = Logger.getLogger(this.getClass)

  class ParseArgsException(s: String) extends Exception(s) {}

  //ACTION:etl,all,jobs,med,cli,are
  var ACTION = ""
  val PACKAGENAMES: Seq[String] = Seq()
  //  val PACKAGE_NAME: String = "%s".format(this.getClass.getPackage.getName.replaceFirst("driver", "connector"))

  val hmErrors: util.Map[String, String] = new util.HashMap[String, String]()
  hmErrors.put("E001", "ERROR :: E001 - Missing parameter - %s=?")
  hmErrors.put("E002", "ERROR :: E002 - Dependency missing - %s")
  hmErrors.put("E003", "ERROR :: E003 - Illegal parameter exception - %s Must be:etl\\all\\jobs\\med\\cli\\are ")


  def getError(sCode: String, sBuild: String): String = hmErrors.get(sCode).format(sBuild)


  def parserArgs(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new ParseArgsException(this.getError("E001", "action"))
    }

    for (i <- args.indices) {
      val argsSplit: Array[String] = args(i).split("=")

      val prefArg: String = argsSplit(0).toLowerCase()

      val posfArgStr: String = argsSplit(1)
      //val posfArg: Array[String] = argsSplit( 1 ).split(",").map(_.toUpperCase)

      prefArg match {
        case "action" => ACTION = posfArgStr

        case _ =>
      }
    }
    logger.warn("INFO :: %s".format(this.toString))
    this._CheckArgs()
    //etl,all,jobs,med,cli,are
    ACTION.toLowerCase() match {
      case "etl" => PACKAGENAMES :+ Constant.ETL
      case "all" => PACKAGENAMES :+ Constant.ETL:+ Constant.ARE :+ Constant.CLI +: Constant.MED
      case "jobs" => PACKAGENAMES :+ Constant.ARE :+ Constant.CLI +: Constant.MED
      case "med" => PACKAGENAMES :+ Constant.MED
      case "cli" => PACKAGENAMES :+ Constant.CLI
      case "are" => PACKAGENAMES :+ Constant.ARE
      case _ => throw new ParseArgsException(this.getError("E003", "action"))
    }
  }

  private def _CheckArgs(): Unit = {
    // STOP APPLICATION IF MANDATORY PARAMETERS ARE NOT PRESENT
    if (ACTION.equalsIgnoreCase("")) throw new ParseArgsException(this.getError("E001", "action"))


  }


  override def toString = s"ApplicationProperties($ACTION, $PACKAGENAMES)"
}
