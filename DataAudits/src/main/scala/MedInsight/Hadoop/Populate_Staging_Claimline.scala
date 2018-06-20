package MedInsight.Hadoop

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by christopher.workman on 6/17/2018.
  */
class Populate_Staging_Claimline (ss: SparkSession, miConfig: MIConfig, auditDF : DataFrame, baseStagingClaimlinDF: DataFrame) {
  def populate(): DataFrame = {
    val bsDF = baseStagingClaimlinDF

    val outputDF = bsDF.join(  auditDF,
                               bsDF("CLAIM_ID")       <=> auditDF("CLAIM_ID")
                               && bsDF("CL_DATA_SRC") <=> auditDF("DATA_SOURCE")
                               && bsDF("FROM_DATE")   <=> auditDF("SVYEARMO"),
                               "left_outer"
       )
      .select( bsDF("CLAIM_ID"),
        bsDF("CL_DATA_SRC"),
        bsDF("FROM_DATE"),
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
        bsDF("MI_USER_DIM_01_"),
        bsDF("MI_USER_DIM_02_"),
        bsDF("MI_USER_DIM_03_"),
        bsDF("MI_USER_DIM_04_"),
        bsDF("MI_USER_DIM_05_"),
        bsDF("MI_USER_DIM_06_"),
        bsDF("MI_USER_DIM_07_"),
        bsDF("MI_USER_DIM_08_"),
        bsDF("MI_USER_DIM_09_"),
        bsDF("MI_USER_DIM_10_"),
        bsDF("MS_DRG"),
        bsDF("TRI_DRG"),
        bsDF("AP_DRG"),
        bsDF("APR_DRG"),
        bsDF("APR_DRG_SEV"),
        bsDF("ICD_10_OR_HIGHER"),
        bsDF("ICD_DIAG_01"),
        bsDF("ICD_DIAG_02"),
        bsDF("ICD_DIAG_03"),
        bsDF("ICD_DIAG_04"),
        bsDF("ICD_DIAG_05"),
        bsDF("ICD_DIAG_06"),
        bsDF("ICD_DIAG_07"),
        bsDF("ICD_DIAG_08"),
        bsDF("ICD_DIAG_09"),
        bsDF("ICD_DIAG_10"),
        bsDF("ICD_PROC_01"),
        bsDF("ICD_PROC_02"),
        bsDF("ICD_PROC_03"),
        bsDF("ICD_PROC_04"),
        bsDF("ICD_PROC_05"),
        bsDF("ICD_PROC_06"),
        bsDF("ICD_PROC_07"),
        bsDF("ICD_PROC_08"),
        bsDF("NDC"))


    println("Sample output for PopulateStagingClaimline " + outputDF.show(10))
    return outputDF
  }
}
