package MedInsight.Hadoop

import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{concat, _}

/**
  * Created by christopher.workman on 6/17/2018.
  */
class Populate_Staging_Claimline (ss: SparkSession, miConfig: MIConfig, auditDF : DataFrame, baseStagingClaimlinDF: DataFrame) {
  def populate(): DataFrame = {
    val bsDF = baseStagingClaimlinDF

    val outputDF = bsDF.join(  auditDF,
                               bsDF("CLAIM_ID")       === auditDF("CLAIM_ID")
                               && bsDF("CL_DATA_SRC") === auditDF("DATA_SOURCE")
                               &&  concat(substring(bsDF("FROM_DATE").cast(DataTypes.StringType),1,4),substring(bsDF("FROM_DATE").cast(DataTypes.StringType),6,2))  >= auditDF("SVYEARMO"),
                               "left_outer"
       )
      .select( bsDF("CLAIM_ID"),
        bsDF("CL_DATA_SRC"),

       concat(substring(bsDF("FROM_DATE").cast(DataTypes.StringType),1,4),substring(bsDF("FROM_DATE").cast(DataTypes.StringType),6,2)).as("YEARMO"),
/*        bsDF("FROM_DATE").as("YEARMO"),*/
        auditDF("SVYEARMO"),
        auditDF("row_type").as("ROW_TYPE"),
        bsDF("CLAIM_SFX_OR_PARENT"),
        bsDF("SV_LINE"),
        bsDF("FORM_TYPE"),
        bsDF("BILL_PROV"),
        bsDF("SV_STAT"),
        bsDF("DIS_STAT"),
        bsDF("SV_UNITS"),
        bsDF("REV_CODE"),
        bsDF("PROC_CODE"),
        bsDF("POS"),
        bsDF("ATT_PROV"),
        bsDF("ATT_PROV_SPEC"),
        bsDF("CLAIM_IN_NETWORK"),
        bsDF("FROM_DATE"),
        bsDF("ADM_DATE"),
        bsDF("TO_DATE"),
        bsDF("DIS_DATE"),
        bsDF("PAID_DATE"),
        bsDF("MEMBER_ID"),
        bsDF("MEMBER_QUAL"),
        bsDF("AMT_BILLED"),
        bsDF("AMT_ALLOWED"),
        bsDF("AMT_PAID"),
        bsDF("AMT_DEDUCT"),
        bsDF("AMT_COINS"),
        bsDF("AMT_COPAY"),
        bsDF("AMT_COB"),
        bsDF("RX_DAYS_SUPPLY"),
        bsDF("NDC"))

    return outputDF
  }
}
