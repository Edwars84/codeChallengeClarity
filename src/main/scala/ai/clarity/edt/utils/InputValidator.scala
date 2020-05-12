package ai.clarity.edt.utils

import java.nio.file.{Files, Paths}
import java.text.{ParseException, SimpleDateFormat}

import ai.clarity.edt.constants.Keys._

object InputValidator {

  private val FormatDate = "yyyy-MM-dd'T'HH:mm:ss"

  /** Validates args received by [[ai.clarity.edt.launchers.ParseDataWithTimeLauncher]]
   *
   * @param args
   * @return Either[Error,Map[String, String]] depending on if args has passed the input validations.*/
  def parseDataValidator(args: Array[String]): Either[Error,Map[String, String]] = {

    if(args.length != 4) return Left(new Error("Arguments number must be 4: Log file path, init_datetime, end_datetime, Hostname"))

    if(!Files.exists(Paths.get(args(0)))) return Left(new Error("%s doesn't exist".format(args(0))))

    val initDatetime = parseDatetimeToTimestamp(args(1))
    val initTimestamp = initDatetime match {
      case Left(error) => return Left(new Error("Init datetime: %s".format(error.getMessage)))
      case Right(timestamp) => timestamp
    }

    val endDatetime = parseDatetimeToTimestamp(args(2))
    val endTimestamp = endDatetime match {
      case Left(error) => return Left(new Error("End datetime: %s".format(error.getMessage)))
      case Right(timestamp) => timestamp
    }
    Right(Map(LogPath -> args(0), InitDate -> initTimestamp, EndDate -> endTimestamp, Hostname -> args(3)))
  }

  /** Validates args received by [[ai.clarity.edt.launchers.UnlimitedInputParserLauncher]]
   *
   * @param args
   * @return Either[Error,Map[String, String]] depending on if args has passed the input validations.*/
  def parseUnlimitedInputValidator(args: Array[String]): Either[Error,Map[String, String]] = {

    if(args.length != 3) return Left(new Error("Arguments number must be 3: Log file path, init_datetime, end_datetime"))

    if(!Files.exists(Paths.get(args(0)))) return Left(new Error("%s doesn't exist".format(args(0))))

    Right(Map(LogPath -> args(0), InitDate -> args(1), EndDate -> args(2)))
  }

  /** Transforms a string date with [[FormatDate]] format to a Unix Timestamp
   *
   * @param dateTime
   * @return Either[Error, String] depending on if date format corresponds to [[FormatDate]]
   */
  def parseDatetimeToTimestamp(dateTime: String): Either[Error, String] = {

    val dateFormat = new SimpleDateFormat(FormatDate)
    try {
      val dateAux = dateFormat.parse(dateTime)
      return Right(dateAux.getTime.toString)
    } catch {
        case e: ParseException => return Left(new Error("format date must be %s".format(FormatDate)))
    }
  }
}
