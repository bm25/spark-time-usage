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
    val df = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    val dfOfIncorrectTotalTimePerDay = df
          .withColumn("sumTimeUsagePerRow", expr("primaryNeeds + work + other"))
          .where("(sumTimeUsagePerRow <= 0 or sumTimeUsagePerRow >= 24.1)")

    dfOfIncorrectTotalTimePerDay.show()

    assert(dfOfIncorrectTotalTimePerDay.count() == 0, "There are rows with probably incorrect total time per day ")
  }

  @Test def `'timeUsageGrouped' should return 12 rows of 6 columns`: Unit = {
    assert(initializeTimeUsage(), " -- initialization failed")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    var df = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    df = timeUsageGrouped(df)
    assert(df.count() == 12, "there should be 12 rows in final dataset")
    assert(df.columns.size == 6, "there should be 12 rows in final dataset")
  }

  @Test def `'timeUsageGroupedSql' should return 12 rows of 6 columns`: Unit = {
    assert(initializeTimeUsage(), " -- initialization failed")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    var df = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    df = timeUsageGroupedSql(df)
    assert(df.count() == 12, "there should be 12 rows in final dataset")
    assert(df.columns.size == 6, "there should be 12 rows in final dataset")
  }

  @Test def `'timeUsageGroupedTyped' should return 12 rows of 6 columns`: Unit = {
    assert(initializeTimeUsage(), " -- initialization failed")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val df = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    var ds = timeUsageSummaryTyped(df)
    ds = timeUsageGroupedTyped(ds)
    ds.show()
    assert(ds.count() == 12, "there should be 12 rows in final dataset")
    assert(ds.columns.size == 6, "there should be 12 rows in final dataset")
  }
}
