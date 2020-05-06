package timeusage

import org.apache.spark.sql.{DataFrame, _}

import scala.collection.mutable.ListBuffer

import scala.io.Source

/** Main class */
object TimeUsage extends TimeUsageInterface {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  val COMMA_SEPARATED_COLUMNS = "tucaseid,gemetsta,gtmetsta,peeduca,pehspnon,ptdtrace,teage,telfs,temjot,teschenr,teschlvl,tesex,tespempnot,trchildnum,trdpftpt,trernwa,trholiday,trspftpt,trsppres,tryhhchild,tudiaryday,tufnwgtp,tehruslt,tuyear,t010101,t010102,t010199,t010201,t010299,t010301,t010399,t010401,t010499,t010501,t010599,t019999,t020101,t020102,t020103,t020104,t020199,t020201,t020202,t020203,t020299,t020301,t020302,t020303,t020399,t020401,t020402,t020499,t020501,t020502,t020599,t020681,t020699,t020701,t020799,t020801,t020899,t020901,t020902,t020903,t020904,t020905,t020999,t029999,t030101,t030102,t030103,t030104,t030105,t030108,t030109,t030110,t030111,t030112,t030186,t030199,t030201,t030202,t030203,t030204,t030299,t030301,t030302,t030303,t030399,t030401,t030402,t030403,t030404,t030405,t030499,t030501,t030502,t030503,t030504,t030599,t039999,t040101,t040102,t040103,t040104,t040105,t040108,t040109,t040110,t040111,t040112,t040186,t040199,t040201,t040202,t040203,t040204,t040299,t040301,t040302,t040303,t040399,t040401,t040402,t040403,t040404,t040405,t040499,t040501,t040502,t040503,t040504,t040505,t040506,t040507,t040508,t040599,t049999,t050101,t050102,t050103,t050189,t050201,t050202,t050203,t050204,t050289,t050301,t050302,t050303,t050304,t050389,t050403,t050404,t050405,t050481,t050499,t059999,t060101,t060102,t060103,t060104,t060199,t060201,t060202,t060203,t060289,t060301,t060302,t060303,t060399,t060401,t060402,t060403,t060499,t069999,t070101,t070102,t070103,t070104,t070105,t070199,t070201,t070299,t070301,t070399,t079999,t080101,t080102,t080199,t080201,t080202,t080203,t080299,t080301,t080302,t080399,t080401,t080402,t080403,t080499,t080501,t080502,t080599,t080601,t080602,t080699,t080701,t080702,t080799,t080801,t080899,t089999,t090101,t090102,t090103,t090104,t090199,t090201,t090202,t090299,t090301,t090302,t090399,t090401,t090402,t090499,t090501,t090502,t090599,t099999,t100101,t100102,t100103,t100199,t100201,t100299,t100381,t100383,t100399,t100401,t100499,t109999,t110101,t110199,t110281,t110289,t119999,t120101,t120199,t120201,t120202,t120299,t120301,t120302,t120303,t120304,t120305,t120306,t120307,t120308,t120309,t120310,t120311,t120312,t120313,t120399,t120401,t120402,t120403,t120404,t120405,t120499,t120501,t120502,t120503,t120504,t120599,t129999,t130101,t130102,t130103,t130104,t130105,t130106,t130107,t130108,t130109,t130110,t130111,t130112,t130113,t130114,t130115,t130116,t130117,t130118,t130119,t130120,t130121,t130122,t130123,t130124,t130125,t130126,t130127,t130128,t130129,t130130,t130131,t130132,t130133,t130134,t130135,t130136,t130199,t130201,t130202,t130203,t130204,t130205,t130206,t130207,t130208,t130209,t130210,t130211,t130212,t130213,t130214,t130215,t130216,t130217,t130218,t130219,t130220,t130221,t130222,t130223,t130224,t130225,t130226,t130227,t130228,t130229,t130230,t130231,t130232,t130299,t130301,t130302,t130399,t130401,t130402,t130499,t139999,t140101,t140102,t140103,t140104,t140105,t149999,t150101,t150102,t150103,t150104,t150105,t150106,t150199,t150201,t150202,t150203,t150204,t150299,t150301,t150302,t150399,t150401,t150402,t150499,t150501,t150599,t150601,t150602,t150699,t159989,t160101,t160102,t160103,t160104,t160105,t160106,t160107,t160108,t169989,t180101,t180199,t180280,t180381,t180382,t180399,t180481,t180482,t180499,t180501,t180502,t180589,t180601,t180682,t180699,t180701,t180782,t180801,t180802,t180803,t180804,t180805,t180806,t180807,t180899,t180901,t180902,t180903,t180904,t180905,t180999,t181002,t181081,t181099,t181101,t181199,t181201,t181202,t181204,t181283,t181299,t181301,t181302,t181399,t181401,t181499,t181501,t181599,t181601,t181699,t181801,t181899,t189999,t500101,t500103,t500104,t500105,t500106,t500107,t509989"
  val PATH_TO_DATA = "src/main/resources/timeusage/atussum.csv"
  val PATH_TO_RES_COLUMN_NAMES = "/timeusage/column-names.csv"
  val COLUMN_NAMES = readColumnNamesFromString(COMMA_SEPARATED_COLUMNS)

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .master("local")
      .getOrCreate()

  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {
    timeUsageByLifePeriod()
    spark.close()
  }

  def readColumnNamesFromFile(path: String): List[String] = {
    Option(getClass.getResourceAsStream(path)) match {
      case None => sys.error("column-names.csv file is not provided")
      case Some(resource) => {
        val src = Source.fromInputStream(resource)
        try {
          src.getLines().toList(0).split(",").toList
        }
        finally {
          src.close()
        }
      }
    }
  }

  def readColumnNamesFromString(commaSeparatedColumns: String): List[String] = {
    commaSeparatedColumns.split(",").toList
  }

  def timeUsageByLifePeriod(): Unit = {
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(COLUMN_NAMES)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, read(PATH_TO_DATA)._2)
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
  def classifiedColumns(columnNames: List[String]): (List[Column], List[Column], List[Column]) = {
    val primaryNeedsRegex = "^t(01|03|11|1801|1803).*"
    val workingActivitiesRegex = "^t(05|1805).*"
    val leisureRegex = "^t(02|04|06|07|08|09|10|12|13|14|15|16|18).*"

    var primaryNeeds = new ListBuffer[Column]()
    var workingActivities = new ListBuffer[Column]()
    var leisure = new ListBuffer[Column]()

    columnNames.foreach(columnName => {
      columnName match {
        case s if s.matches(primaryNeedsRegex) => {
          primaryNeeds += col(columnName)
        }
        case s if s.matches(workingActivitiesRegex) => {
          workingActivities += col(columnName);
        }
        case s if s.matches(leisureRegex) => {
          leisure += col(columnName);
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
