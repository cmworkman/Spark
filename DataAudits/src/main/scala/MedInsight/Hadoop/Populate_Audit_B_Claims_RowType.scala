package MedInsight.Hadoop

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by christopher.workman on 6/17/2018.
  */
class Populate_Audit_B_Claims_RowType(ss: SparkSession, miConfig: MIConfig, baseStagingClaimlineDF: DataFrame) {
  def populate() : DataFrame = {
    val clDF = baseStagingClaimlineDF

    val bcDF = clDF.select(
      coalesce(clDF("CL_DATA_SRC"), lit("")).as("DATA_SOURCE"),
      when(clDF("FORM_TYPE") === "U",
        (when(
          ((clDF("MS_DRG").isNotNull) && (clDF("MS_DRG") !== "") && (clDF("MS_DRG") !== "0")) ||
            clDF("POS").isin(Seq("21", "25", "51", "61"): _*) ||
            clDF("UB_BILL_TYPE").isin(Seq("11", "12", "17", "18", "21", "22", "27", "28"): _*), "1HIP").otherwise("2HOP"))
      ).
        when(clDF("FORM_TYPE") === "H", "3PHY").
        when(clDF("FORM_TYPE") === "D", "4RX").
        when(clDF("FORM_TYPE") === "A", "5DEN").
        when(clDF("FORM_TYPE") === "V", "6VIS").
        when(clDF("FORM_TYPE") === "L", "7LAB").
        otherwise("ERR").as("ROW_TYPE"),
      clDF("CLAIM_ID"),
      clDF("AMT_BILLED"),
      clDF("AMT_DEDUCT"),
      clDF("AMT_ALLOWED"),
      clDF("AMT_COPAY"),
      clDF("AMT_PAID"),
      clDF("AMT_COINS"),
      clDF("AMT_COB"),
      clDF("MEMBER_ID"),
      clDF("MEMBER_QUAL"),
      when(clDF("FROM_DATE").isin(miConfig.proxyNullDates: _*) || clDF("FROM_DATE").isin(miConfig.proxyOpenDates: _*), "-NULL-").otherwise(coalesce(clDF("FROM_DATE"), clDF("DIS_DATE"))).as("SVYEARMO")
    )

    val outputDF = bcDF.groupBy(bcDF("DATA_SOURCE"), bcDF("CLAIM_ID"))
      .agg(max(bcDF("member_qual")).as("MEMBER_QUAL"), max(bcDF("member_id")).as("MEMBER_ID"),
        sum(bcDF("AMT_BILLED")).as("AMT_BILLED"), sum(bcDF("AMT_PAID")).as("AMT_PAID"), sum(bcDF("AMT_ALLOWED")).as("AMT_ALLOWED"), sum(bcDF("AMT_DEDUCT")).as("AMT_DEDUCT"), sum(bcDF("AMT_COPAY")).as("AMT_COPAY"), sum(bcDF("AMT_COINS")).as("AMT_COINS"), sum(bcDF("AMT_COB")).as("AMT_COB"),
        min(bcDF("ROW_TYPE")).as("ROW_TYPE"), min(bcDF("SVYEARMO")).as("SVYEARMO"),
        count(bcDF("ROW_TYPE")).as("LINES")
      )

    return outputDF

  }
}
