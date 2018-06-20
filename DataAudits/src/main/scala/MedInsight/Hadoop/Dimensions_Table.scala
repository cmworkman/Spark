package MedInsight.Hadoop

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * Created by christopher.workman on 6/19/2018.
  */
class Dimensions_Table {
  val DIMENSION = StructField("DDIMENSION ", DataTypes.StringType)
  val BUSINESS_NAME = StructField("BUSINESS_NAME", DataTypes.StringType)
  val CLAIMS_PRIMARY = StructField("CLAIMS_PRIMARY", DataTypes.StringType)
  val ENROLL_PRIMARY = StructField("ENROLL_PRIMARY", DataTypes.StringType)
  val IBNR_DIM = StructField("IBNR_DIM", DataTypes.StringType)
  val BENCHMARK_DIM = StructField("BENCHMARK_DIM", DataTypes.StringType)
  val MI65_ENABLED = StructField("MI65_ENABLED", DataTypes.StringType)
  val REPORT_ENABLED = StructField("REPORT_ENABLED", DataTypes.StringType)
  val AS_CUBE_ENALBED = StructField("AS_CUBE_ENALBED", DataTypes.StringType)
  val DI_CUBE_GROUPING = StructField("DI_CUBE_GROUPING", DataTypes.StringType)
  val ACO_HIERARCHY = StructField("CO_HIERARCHY", DataTypes.LongType)


  val fields = Array(DIMENSION, BUSINESS_NAME, CLAIMS_PRIMARY, ENROLL_PRIMARY, IBNR_DIM, MI65_ENABLED, REPORT_ENABLED, AS_CUBE_ENALBED, DI_CUBE_GROUPING, ACO_HIERARCHY)
  val schema = StructType(fields)
}