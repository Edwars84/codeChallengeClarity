package ai.clarity.edt.launchers

import ai.clarity.edt.Core
import ai.clarity.edt.utils.InputValidator
import org.apache.spark.internal.Logging

/** Launcher for the first goal to achieve (Parse the data with a time_init, time_end) */
object ParseDataWithTimeLauncher extends SparkApp with Logging {

  InputValidator.parseDataValidator(args) match {
    case Left(error) => {
      log.error(error.getMessage)
      finishingWithErrors
    }
    case Right(inputMap) => Core.firstGoal(inputMap)
  }
}
