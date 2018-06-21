package MedInsight.Hadoop

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * Created by christopher.workman on 6/17/2018.
  */
class Dates_Table {
  val DATES  = StructField("DATES", DataTypes.DateType)
  val YEARS  = StructField("YEARS", DataTypes.LongType)
  val MONTHS = StructField("MONTHS", DataTypes.StringType)
  val MONTH_NAME  = StructField("MONTH_NAME", DataTypes.StringType)
  val YEAR_MO  = StructField("YEAR_MO", DataTypes.LongType)
  val FIRST_DATE_IN_MONTH  = StructField("FIRST_DATE_IN_MONTH", DataTypes.StringType)
  val LAST_DATE_IN_MONTH  = StructField("LAST_DATE_IN_MONTH", DataTypes.StringType)
  val DAY_OF_MONTH  = StructField("DAY_OF_MONTH", DataTypes.ShortType)
  val WEEKEND_FLAG  = StructField("WEEKEND_FLAG", DataTypes.ShortType)
  val DAY_OF_WEEK  = StructField("DAY_OF_WEEK", DataTypes.ShortType)
  val DAY_OF_YEAR  = StructField("DAY_OF_YEAR", DataTypes.LongType)
  val JULIAN_DATE  = StructField("JULIAN_DATE", DataTypes.LongType)
  val DAY_NAME  = StructField("DAY_NAME", DataTypes.StringType)
  val CALENDAR_QUARTER = StructField("CALENDAR_QUARTER", DataTypes.ShortType)
  val FISCAL_QUARTER  = StructField("FISCAL_QUARTER", DataTypes.StringType)
  val SEASON  = StructField("SEASON", DataTypes.StringType)
  val HOLIDAY_FLAG  = StructField("HOLIDAY_FLAG", DataTypes.StringType)
  val FISCAL_YEAR  = StructField("FISCAL_YEAR", DataTypes.StringType)


  val fields = Array(DATES,YEARS,MONTHS,MONTH_NAME,YEAR_MO,FIRST_DATE_IN_MONTH,LAST_DATE_IN_MONTH,DAY_OF_MONTH,WEEKEND_FLAG,DAY_OF_WEEK,DAY_OF_YEAR,JULIAN_DATE,DAY_NAME,CALENDAR_QUARTER,FISCAL_QUARTER,SEASON,HOLIDAY_FLAG,FISCAL_YEAR)
  val schema = StructType(fields)

}
