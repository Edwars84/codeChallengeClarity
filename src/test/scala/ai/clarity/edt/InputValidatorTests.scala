package ai.clarity.edt

import ai.clarity.edt.utils.InputValidator
import org.scalatest.funsuite.AnyFunSuite

class InputValidatorTests extends AnyFunSuite {

  test("Testing parseDatetimeToTimestamp yyyy-MM-dd'T'HH:mm:ss"){
    val date = "2019-08-13T23:59:59"
    val result = InputValidator.parseDatetimeToTimestamp(date)

    assert(result.isRight)
    assert(result.right.get == "1565733599000")
  }

  test("Testing parseDatetimeToTimestamp yyyy-MM-dd"){
    val date = "2019-08-13"
    val result = InputValidator.parseDatetimeToTimestamp(date)
    assert(result.isLeft)
  }
}
