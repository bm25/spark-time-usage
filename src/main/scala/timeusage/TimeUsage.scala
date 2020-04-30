package timeusage

import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

/** Main class */
object TimeUsage extends TimeUsageInterface {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .master("local")
      .getOrCreate()

  private var columns: List[String] = null
  private var initDf: DataFrame = null

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  def getColumns: List[String] = {
    if (columns == null) {
      val readCsv = read("src/main/resources/timeusage/atussum.csv")
      columns = readCsv._1
      initDf = readCsv._2
    }

    columns
  }

  def getInitDf: DataFrame = {
    if (initDf == null) {
      val readCsv = read("src/main/resources/timeusage/atussum.csv")
      columns = readCsv._1
      initDf = readCsv._2
    }

    initDf
  }

  /** Main function */
  def main(args: Array[String]): Unit = {
    timeUsageByLifePeriod()
    spark.close()
  }

  def timeUsageByLifePeriod(): Unit = {
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(getColumns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, getInitDf)
    val finalDf = timeUsageGrouped(summaryDf)
    finalDf.show()
  }

  /** @return The read DataFrame along with its column names. */
  def read(path: String): (List[String], DataFrame) = {
    val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(path)
    (df.schema.fields.map(_.name).toList, df)
  }

  /** @return An RDD Row compatible with the schema produced by `dfSchema`
    * @param line Raw fields
    */
  def row(line: List[String]): Row =
    Row.fromSeq(line)

  /** @return The initial data frame columns partitioned in three groups: primary needs (sleeping, eating, etc.),
    *         work and other (leisure activities)
    *
    * @see https://www.kaggle.com/bls/american-time-use-survey
    *
    * The dataset contains the daily time (in minutes) people spent in various activities. For instance, the column
    * “t010101” contains the time spent sleeping, the column “t110101” contains the time spent eating and drinking, etc.
    *
    * This method groups related columns together:
    * 1. “primary needs” activities (sleeping, eating, etc.). These are the columns starting with “t01”, “t03”, “t11”,
    *    “t1801” and “t1803”.
    * 2. working activities. These are the columns starting with “t05” and “t1805”.
    * 3. other activities (leisure). These are the columns starting with “t02”, “t04”, “t06”, “t07”, “t08”, “t09”,
    *    “t10”, “t12”, “t13”, “t14”, “t15”, “t16” and “t18” (those which are not part of the previous groups only).
    */
  def classifiedColumns(columnNames: List[String] /*, df: DataFrame */): (List[Column], List[Column], List[Column]) = {
    val primaryNeedsRegex = "^t(01|03|11|1801|1803).*"
    val workingActivitiesRegex = "^t(05|1805).*"
    val leisureRegex = "^t(02|04|06|07|08|09|10|12|13|14|15|16|18).*"

    var primaryNeeds = new ListBuffer[Column]()
    var workingActivities = new ListBuffer[Column]()
    var leisure = new ListBuffer[Column]()
    val df = getInitDf

    columnNames.foreach(columnName => {
      columnName match {
        case s if s.matches(primaryNeedsRegex) => {
          primaryNeeds += df.col(columnName)
        }
        case s if s.matches(workingActivitiesRegex) => {
          workingActivities += df.col(columnName);
        }
        case s if s.matches(leisureRegex) => {
          leisure += df.col(columnName);
        }
        case _ => {
          println("case-4: No matching for columnName = " + columnName)
        }
      }
    })

    (primaryNeeds.toList, workingActivities.toList, leisure.toList)
  }

  /** @return a projection of the initial DataFrame such that all columns containing hours spent on primary needs
    *         are summed together in a single column (and same for work and leisure). The “teage” column is also
    *         projected to three values: "young", "active", "elder".
    *
    * @param primaryNeedsColumns List of columns containing time spent on “primary needs”
    * @param workColumns List of columns containing time spent working
    * @param otherColumns List of columns containing time spent doing other activities
    * @param df DataFrame whose schema matches the given column lists
    *
    * This methods builds an intermediate DataFrame that sums up all the columns of each group of activity into
    * a single column.
    *
    * The resulting DataFrame should have the following columns:
    * - working: value computed from the “telfs” column of the given DataFrame:
    *   - "working" if 1 <= telfs < 3
    *   - "not working" otherwise
    * - sex: value computed from the “tesex” column of the given DataFrame:
    *   - "male" if tesex = 1, "female" otherwise
    * - age: value computed from the “teage” column of the given DataFrame:
    *   - "young" if 15 <= teage <= 22,
    *   - "active" if 23 <= teage <= 55,
    *   - "elder" otherwise
    * - primaryNeeds: sum of all the `primaryNeedsColumns`, in hours
    * - work: sum of all the `workColumns`, in hours
    * - other: sum of all the `otherColumns`, in hours
    *
    * Finally, the resulting DataFrame should exclude people that are not employable (ie telfs = 5).
    *
    * Note that the initial DataFrame contains time in ''minutes''. You have to convert it into ''hours''.
    */
  def timeUsageSummary(
    primaryNeedsColumns: List[Column],
    workColumns: List[Column],
    otherColumns: List[Column],
    df: DataFrame
  ): DataFrame = {
    // Transform the data from the initial dataset into data that make
    // more sense for our use case
    // Hint: you can use the `when` and `otherwise` Spark functions
    // Hint: don’t forget to give your columns the expected name with the `as` method

    df
      .withColumn("working",
        when(col("telfs") >= 1 && col("telfs") < 3, "working")
          .otherwise("not working"))
      .withColumn("sex",
        when(col("tesex") === "1", "male")
          .otherwise("female"))
      .withColumn("age",
        expr("case " +
          "when (teage >= 15 and teage <= 22) then 'young' " +
          "when (teage >= 23 and teage <= 55) then 'active' " +
          "else 'elder' end")
      )
      .withColumn("primaryNeeds", primaryNeedsColumns.reduce((c1, c2) => c1 + c2).as[Double] / 60)
      .withColumn("work", workColumns.reduce((c1, c2) => c1 + c2).as[Double] / 60)
      .withColumn("other", otherColumns.reduce((c1, c2) => c1 + c2).as[Double] / 60)
      .select("working","sex","age","primaryNeeds","work","other")
      .where($"telfs" <= 4) // Discard people who are not in labor force
  }

  /** @return the average daily time (in hours) spent in primary needs, working or leisure, grouped by the different
    *         ages of life (young, active or elder), sex and working status.
    * @param summed DataFrame returned by `timeUsageSumByClass`
    *
    * The resulting DataFrame should have the following columns:
    * - working: the “working” column of the `summed` DataFrame,
    * - sex: the “sex” column of the `summed` DataFrame,
    * - age: the “age” column of the `summed` DataFrame,
    * - primaryNeeds: the average value of the “primaryNeeds” columns of all the people that have the same working
    *   status, sex and age, rounded with a scale of 1 (using the `round` function),
    * - work: the average value of the “work” columns of all the people that have the same working status, sex
    *   and age, rounded with a scale of 1 (using the `round` function),
    * - other: the average value of the “other” columns all the people that have the same working status, sex and
    *   age, rounded with a scale of 1 (using the `round` function).
    *
    * Finally, the resulting DataFrame should be sorted by working status, sex and age.
    */
  def timeUsageGrouped(summed: DataFrame): DataFrame = {
    summed.groupBy($"working", $"sex", $"age")
      .agg(round(avg("primaryNeeds"), 1).as("primaryNeeds"), round(avg("work"), 1).as("work"), round(avg("other"), 1).as("other"))
      .orderBy($"working", $"sex", $"age")
  }

  /**
    * @return Same as `timeUsageGrouped`, but using a plain SQL query instead
    * @param summed DataFrame returned by `timeUsageSumByClass`
    */
  def timeUsageGroupedSql(summed: DataFrame): DataFrame = {
    val viewName = s"summed"
    summed.createOrReplaceTempView(viewName)
    spark.sql(timeUsageGroupedSqlQuery(viewName))
  }

  /** @return SQL query equivalent to the transformation implemented in `timeUsageGrouped`
    * @param viewName Name of the SQL view to use
    */
  def timeUsageGroupedSqlQuery(viewName: String): String =
    "SELECT working, sex, age, round(avg(primaryNeeds), 1) as primaryNeeds, round(avg(work), 1) as work, round(avg(other), 1) as other " +
      "FROM summed " +
      "GROUP BY working, sex, age " +
      "ORDER BY working, sex, age"

  /**
    * @return A `Dataset[TimeUsageRow]` from the “untyped” `DataFrame`
    * @param timeUsageSummaryDf `DataFrame` returned by the `timeUsageSummary` method
    *
    * Hint: you should use the `getAs` method of `Row` to look up columns and
    * cast them at the same time.
    */
  def timeUsageSummaryTyped(timeUsageSummaryDf: DataFrame): Dataset[TimeUsageRow] = {
    val encoder = org.apache.spark.sql.Encoders.product[TimeUsageRow]
    timeUsageSummaryDf.as(encoder)
  }

  /**
    * @return Same as `timeUsageGrouped`, but using the typed API when possible
    * @param summed Dataset returned by the `timeUsageSummaryTyped` method
    *
    * Note that, though they have the same type (`Dataset[TimeUsageRow]`), the input
    * dataset contains one element per respondent, whereas the resulting dataset
    * contains one element per group (whose time spent on each activity kind has
    * been aggregated).
    *
    * Hint: you should use the `groupByKey` and `typed.avg` methods.
    */
  def timeUsageGroupedTyped(summed: Dataset[TimeUsageRow]): Dataset[TimeUsageRow] = {
    import org.apache.spark.sql.expressions.scalalang.typed
    summed
      .groupByKey(row => (row.working, row.sex, row.age))
      .agg(
        round(typed.avg[TimeUsageRow](_.primaryNeeds),1).as[Double],
        round(typed.avg[TimeUsageRow](_.work),1).as[Double],
        round(typed.avg[TimeUsageRow](_.other),1).as[Double]
      )
      //.toDF("productId", "sum")
      .map(row => TimeUsageRow(row._1._1, row._1._2, row._1._3, row._2, row._3, row._4))
      .orderBy("working", "sex","age")
  }
}

/**
  * Models a row of the summarized data set
  * @param working Working status (either "working" or "not working")
  * @param sex Sex (either "male" or "female")
  * @param age Age (either "young", "active" or "elder")
  * @param primaryNeeds Number of daily hours spent on primary needs
  * @param work Number of daily hours spent on work
  * @param other Number of daily hours spent on other activities
  */
case class TimeUsageRow(
  working: String,
  sex: String,
  age: String,
  primaryNeeds: Double,
  work: Double,
  other: Double
)
