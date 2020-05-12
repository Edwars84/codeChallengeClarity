package ai.clarity.edt

import ai.clarity.edt.constants.Keys._
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite

class CoreTests extends AnyFunSuite with SharedSparkContext {

  test("Testing getInfoFiltered"){
    import spark.implicits._
    val dataDF = sc.parallelize(Seq(("0","a","b"),("1","a","b"),("2","c","b"),("4","e","d"))).toDF(columnKeys: _*)
    val inputMap = Map(InitDate -> "1", EndDate -> "3")

    val result = Core.getInfoFiltered(inputMap, dataDF)

    assert(result.columns.size == 3)
    assert(result.collect().size == 2)
    assert(!result.collectAsList().contains(Row("0","a","b")))
    assert(result.collectAsList().contains(Row("1","a","b")))
    assert(result.collectAsList().contains(Row("2","c","b")))
    assert(!result.collectAsList().contains(Row("4","e","d")))
  }

  test("Testing getHostsConnectedTo"){
    import spark.implicits._
    val dataDF = sc.parallelize(Seq(("0","a","b"),("1","a","b"),("2","c","b"),("4","e","d"))).toDF(columnKeys: _*)

    val result = Core.getHostsConnectedTo("b", dataDF)

    assert(result.columns.size == 1)
    assert(result.collect().size == 2)
    assert(result.collectAsList().contains(Row("a")))
    assert(result.collectAsList().contains(Row("c")))
    assert(!result.collectAsList().contains(Row("e")))
  }

  test("Testing getHostsConnectedFrom"){
    import spark.implicits._
    val dataDF = sc.parallelize(Seq(("0","a","b"),("1","a","b"),("2","a","c"),("4","e","d"))).toDF(columnKeys: _*)

    val result = Core.getHostsConnectedFrom("a", dataDF)

    assert(result.columns.size == 1)
    assert(result.collect().size == 2)
    assert(result.collectAsList().contains(Row("b")))
    assert(result.collectAsList().contains(Row("c")))
    assert(!result.collectAsList().contains(Row("d")))
  }

  test("Testing getHostMostConnections"){
    import spark.implicits._
    val dataDF = sc.parallelize(Seq(("0","a","b"),("1","a","b"),("2","a","c"),("4","e","d"))).toDF(columnKeys: _*)

    val result = Core.getHostMostConnections(dataDF)

    assert(result.columns.size == 1)
    assert(result.collect().size == 1)
    assert(result.collectAsList().contains(Row("a")))
    assert(!result.collectAsList().contains(Row("e")))
  }
}
