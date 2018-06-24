package MedInsight.Hadoop

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by christopher.workman on 6/23/2018.
  */
class Update_SubscriberID_For_ClaimSummary (ss: SparkSession, miConfig: MIConfig, auditB_MM_EnrollmentDF: DataFrame, claimSummary: DataFrame) {
  def populate() : DataFrame = {
    val ammDF  = auditB_MM_EnrollmentDF
    val csDF = claimSummary


    // I'm making a big assumption here with SUBSCRIBER_ID.  The logic is more complicated in dqa.PopulateProcessingTables (though necessarily?)
    val outputDF = csDF.join(ammDF, ammDF("MEMBER_ID") <=> csDF("MEMBER_ID") && ammDF("MEMBER_QUAL") <=> csDF("MEMBER_QUAL") && ammDF("EN_DATA_SRC") <=> csDF("CL_DATA_SRC"), "left_outer")
                         .select(
                           ammDF("SUBSCRIBER_ID"),
                           csDF("CL_DATA_SRC"), csDF("CLAIM_ID"), csDF("SVYEARMO"), csDF("ROW_TYPE"), csDF("SV_STAT"), csDF("DIS_STAT"),
                           csDF("FROM_DATE"), csDF("ADM_DATE"), csDF("TO_DATE"), csDF("DIS_DATE"), csDF("MEMBER_ID"), csDF("BILL_PROV"),
                           csDF("CLAIM_IN_NETWORK"), csDF("MEMBER_QUAL")
                         )

    return outputDF
  }
}
