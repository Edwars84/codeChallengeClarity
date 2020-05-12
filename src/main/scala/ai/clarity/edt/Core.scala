package ai.clarity.edt

import ai.clarity.edt.constants.Keys._
import ai.clarity.edt.launchers.ParseDataWithTimeLauncher.spark
import ai.clarity.edt.utils.OutputSetup
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, desc, lit}
import org.apache.spark.storage.StorageLevel

object Core {

  val hostTo: String = spark.sparkContext.getConf.get("spark.hostname.To")
  val hostFrom: String = spark.sparkContext.getConf.get("spark.hostname.From")

  /** Logic to get the first goal
   *
   * @param inputMap
   */
  def firstGoal(inputMap: Map[String, String]) = {

    val filteredInfo = getInfoFiltered(inputMap, getInfo(inputMap))
    val hostsConnectedTo = getHostsConnectedTo(inputMap.getOrElse(Hostname, ""), filteredInfo)
    OutputSetup.writeResults(hostsConnectedTo, "firstGoal")
  }

  /** Logic to get the second goal
   *
   * @param inputMap
   */
  def secondGoal(inputMap: Map[String, String]) = {

    val filteredInfo = getInfoFiltered(inputMap, getInfo(inputMap)).persist(StorageLevel.MEMORY_ONLY)

    val hostsConnectedTo = getHostsConnectedTo(hostTo, filteredInfo)
    OutputSetup.writeResults(hostsConnectedTo, "hostsConnectedTo")

    val hostsConnectedFrom = getHostsConnectedFrom(hostFrom, filteredInfo)
    OutputSetup.writeResults(hostsConnectedFrom, "hostsConnectedFrom")

    val hostMostConnections = getHostMostConnections(filteredInfo)
    OutputSetup.writeResults(hostMostConnections, "hostMostConnections")
  }

  /** Reads file
   *
   * @param inputMap
   * @return [[DataFrame]] containing file data
   */
  def getInfo(inputMap: Map[String, String]) = spark.read.option("header", "false").option("delimiter", " ")
    .csv(inputMap.getOrElse(LogPath, "")).toDF(columnKeys: _*)

  /** Filters [[DataFrame]] by timestamp column
   *
   * @param inputMap
   * @param info
   * @return [[DataFrame]] filtered
   */
  def getInfoFiltered(inputMap: Map[String, String], info: DataFrame) =
    info.filter(col("timestamp")
      .between(lit(inputMap.getOrElse(InitDate, "")), lit(inputMap.getOrElse(EndDate, ""))))

  /** Gets hosts which are connected to a given host
   *
   * @param hostname destination
   * @param filteredInfo
   * @return Hosts [[DataFrame]]
   */
  def getHostsConnectedTo(hostname: String, filteredInfo: DataFrame) =
    filteredInfo.filter(col("destination").equalTo(lit(hostname)))
    .select("origin")
    .dropDuplicates()

  /** Gets host which are connected from a given host
   *
   * @param hostname origin
   * @param filteredInfo
   * @return Hosts [[DataFrame]]
   */
  def getHostsConnectedFrom(hostname: String, filteredInfo: DataFrame) =
    filteredInfo.filter(col("origin").equalTo(lit(hostname)))
      .select("destination")
      .dropDuplicates()

  /** Gets host which is connected to more destination hosts
   *
   * @param filteredInfo
   * @return Host [[DataFrame]]
   */
  def getHostMostConnections(filteredInfo: DataFrame) =
    filteredInfo.groupBy(col("origin")).agg(count(col("destination")).as("quantity"))
      .sort(desc("quantity"))
      .limit(1)
      .select("origin")
}
