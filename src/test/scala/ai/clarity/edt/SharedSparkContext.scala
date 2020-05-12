package ai.clarity.edt

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SharedSparkContext extends  BeforeAndAfterAll { self: Suite =>

  @transient private var _sc: SparkContext = _

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Testing")
      .config("spark.master", "local")
      .getOrCreate()

  def sc: SparkContext = _sc

  var conf = new SparkConf(false)

  override def beforeAll() = {
    _sc = spark.sparkContext
    super.beforeAll()
  }

  override def afterAll() = {
    _sc.stop()
    _sc = null
    super.afterAll()
  }
}
