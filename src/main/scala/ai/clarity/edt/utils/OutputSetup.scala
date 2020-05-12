package ai.clarity.edt.utils

import ai.clarity.edt.launchers.ParseDataWithTimeLauncher.spark
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame

object OutputSetup {

  val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val path = spark.sparkContext.getConf.get("spark.custom.result.path")
  val saveMode = spark.sparkContext.getConf.get("spark.custom.saveMode")

  /** Write result in a text file
   *
   * @param data
   * @param caseName
   */
  def writeResults(data: DataFrame, caseName: String): Unit = {

    data.coalesce(1).write.mode(saveMode).text(path+"/tmp")
    OutputSetup.renameHadoopFiles(caseName);
  }

  /** Rename spark name files
   *
   * @param fileName
   * @return
   */
  def renameHadoopFiles(fileName: String) = {

    val files = fs.globStatus(new Path(path+"/tmp/*")).toList.filter( f => f.getPath.getName.endsWith(".txt"))
    files.foreach( file =>
      fs.rename(new Path(path+"/tmp/" + file.getPath.getName), new Path(path+"/%s.txt".format(fileName))));

    fs.delete(new Path(path+"/tmp"), true);
  }
}
