package MedInsight.Hadoop

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit, sum}

/**
  * Created by christopher.workman on 6/23/2018.
  */
class Audit_Attending_Provider_Populate (ss: SparkSession, miConfig: MIConfig, staging_Claimline_DF: DataFrame ) {
  def populate(): DataFrame = {
    val scDF = staging_Claimline_DF

    val outputDF =
      scDF.groupBy(scDF("CL_DATA_SRC"), scDF("ROW_TYPE"), scDF("ATT_PROV"))
        .agg(sum(lit(1)).as("LINES"))

    return outputDF
  }
}
