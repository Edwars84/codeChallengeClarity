package ai.clarity.edt.launchers

import ai.clarity.edt.launchers.ParseDataWithTimeLauncher.spark
import org.apache.spark.sql.SparkSession

/** The `SparkApp` trait is made to combine
 *  `App` trait and a simple spark configuration
 */
trait SparkApp extends App {
  val spark = SparkSession.builder.appName("Code Challenge").getOrCreate()

  val logLevel = spark.sparkContext.getConf.get("spark.custom.log.level")
  spark.sparkContext.setLogLevel(logLevel)

  def finishingWithErrors = {
    spark.sparkContext.stop()
    System.exit(1)
  }
}
