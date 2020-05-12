package ai.clarity.edt.launchers

import ai.clarity.edt.Core
import ai.clarity.edt.utils.InputValidator
import org.apache.spark.internal.Logging

/** Launcher for the second goal to achieve (Unlimited Input Parser) */
object UnlimitedInputParserLauncher extends SparkApp with Logging {

  InputValidator.parseUnlimitedInputValidator(args) match {
    case Left(error) => {
      log.error(error.getMessage)
      finishingWithErrors
    }
    case Right(inputMap) => Core.secondGoal(inputMap)
  }
}
