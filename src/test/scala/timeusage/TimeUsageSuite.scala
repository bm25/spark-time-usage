package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.junit.{Assert, Test}
import org.junit.Assert.assertEquals

import scala.util.Random

class TimeUsageSuite {

  def initializeTimeUsage(): Boolean =
    try {
      TimeUsage
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  import TimeUsage._

  @Test def `'classifiedColumns' should return 3 lists of columns`: Unit = {
    assert(initializeTimeUsage(), " -- initialization failed")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    assert(primaryNeedsColumns.size == 55, "primaryNeedsColumns.size should equal to 55")
    assert(workColumns.size == 23, "workColumns should equal to 23")
    assert(otherColumns.size == 346, "otherColumns should equal to 346")
  }
}
