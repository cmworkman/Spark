package MedInsight.Hadoop

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit, sum, count}

/**
  * Created by christopher.workman on 6/23/2018.
  */
class Audit_Claim_Lines_Populate (ss: SparkSession, miConfig: MIConfig, auditB_ClaimLines_DF: DataFrame ) {
  def populate(): DataFrame = {
    val clDF = auditB_ClaimLines_DF

    val outputDF =
      clDF.groupBy(clDF("DATA_SOURCE"), clDF("SVYEARMO"), clDF("ROW_TYPE"))
            .agg(count(lit(1)).as("CLAIMS"), sum(clDF("LINES")).as("CLAIM_LINES"))

    return outputDF
  }
}