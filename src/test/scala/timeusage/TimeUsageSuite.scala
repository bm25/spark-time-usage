package timeusage

import org.apache.spark.sql.functions.expr
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

  @Test def `'timeUsageSummary' should return total time per row equal to 24 hours`: Unit = {
    assert(initializeTimeUsage(), " -- initialization failed")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)

    val df = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, null)
    val countOfRowsWithIncorrectTotalTimePerDay = df
          .withColumn("sumTimeUsagePerRow", expr("primaryNeeds + work + other"))
          .select("sumTimeUsagePerRow")
          .where("(sumTimeUsagePerRow >= 24.1 or sumTimeUsagePerRow <= 23.9)")
          .count()

    assert(countOfRowsWithIncorrectTotalTimePerDay == 0, "There are rows with incorrect total time per day ")
  }

  @Test def `'timeUsageGrouped' should return 12 rows of 6 columns`: Unit = {
    assert(initializeTimeUsage(), " -- initialization failed")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)

    var df = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, null)
    df = timeUsageGrouped(df)
    assert(df.count() == 12, "there should be 12 rows in final dataset")
    assert(df.columns.size == 6, "there should be 12 rows in final dataset")
  }
}
