package MedInsight.Hadoop

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{coalesce, _}
import org.apache.spark.sql.types.DataTypes

/**
  * Created by christopher.workman on 6/23/2018.
  */
class Audit_Member_Years_Populate (ss: SparkSession, miConfig: MIConfig, auditB_MM_EnrollmentDF: DataFrame ) {
  def populate(): DataFrame = {
    val ammDF = auditB_MM_EnrollmentDF

    // this is awkard, but Spark doesn't support groupBy's without an agg
    val outputDF =
      ammDF.select(ammDF("EN_DATA_SRC"), ammDF("MEMBER_ID"), coalesce(ammDF("MEMBER_QUAL"),lit("")), substring(ammDF("YEAR_MO").cast(DataTypes.StringType),0,4).as("YEAR_MO"), lit(1).as("TOTAL_MEMBER_YEARS")).distinct()

    return outputDF
  }
}