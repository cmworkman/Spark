package MedInsight.Hadoop

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by christopher.workman on 6/23/2018.
  */
class Populate_Month_Count_By_DataSource_In_Audit (ss: SparkSession, miConfig: MIConfig, auditB_MM_EnrollmentDF: DataFrame ) {
  def populate(): DataFrame = {
    val ammDF = auditB_MM_EnrollmentDF


    val outputDF =
      ammDF.groupBy(ammDF("EN_DATA_SRC"), ammDF("YEAR_MO")
      )
      .agg(count(ammDF("YEAR_MO")))

    return outputDF
  }
}
