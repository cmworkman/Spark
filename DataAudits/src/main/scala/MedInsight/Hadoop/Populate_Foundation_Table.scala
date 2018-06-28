package MedInsight.Hadoop

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.types.DataTypes


/**
  * Created by christopher.workman on 6/25/2018.
  */
class Populate_Foundation_Table(ss: SparkSession, miConfig: MIConfig, payerLobDF: DataFrame, payerTypeDF: DataFrame,
                                auditBMMEnrollmentDF: DataFrame, claimSummaryDF: DataFrame) {
  def populate() : DataFrame = {
    val lobDF = payerLobDF
    val ptDF = payerTypeDF
    val abmDF = auditBMMEnrollmentDF
    val csDF = claimSummaryDF

    val consPayerDF = lobDF.join(ptDF, lobDF("PAYER_LOB_KEY") <=> ptDF("PAYER_LOB_KEY"), "left_outer")
      .select(ptDF("PAYER_TYPE"), lobDF("PAYER_LOB")).distinct()


    val enrollTotals = abmDF.join(consPayerDF, consPayerDF("PAYER_TYPE") <=> abmDF("PAYER_TYPE"), "left_outer")
      .groupBy(abmDF("EN_DATA_SRC").as("DATA_SOURCE"), abmDF("YEAR_MO"),
        consPayerDF("PAYER_LOB"),
        coalesce(consPayerDF("PAYER_LOB"), lit("(unknown)"))
      )
      .agg(sum(lit(1)).as("MEMBER_MONTHS"))


    val servTotals = abmDF.join(csDF, csDF("MEMBER_ID") === abmDF("MEMBER_ID") &&
      coalesce(csDF("MEMBER_QUAL"), lit("''")) === coalesce(abmDF("MEMBER_QUAL"), lit("''")) &&
      (csDF("CL_DATA_SRC") === abmDF("EN_DATA_SRC") || csDF("CL_DATA_SRC") === lit("*") || abmDF("EN_DATA_SRC") === lit("*")) &&
      year(csDF("FROM_DATE")) === abmDF("YEAR_MO")
      , "left_outer"
       )
      .join(ptDF, abmDF("PAYER_TYPE") === ptDF("PAYER_TYPE"), "left_outer")
      .join(lobDF, ptDF("PAYER_LOB_KEY") === lobDF("PAYER_LOB_KEY"), "left_outer")
      .groupBy(
        when(csDF("CL_DATA_SRC") === abmDF("EN_DATA_SRC"), csDF("CL_DATA_SRC"))
          .when((csDF("CL_DATA_SRC") === lit("*") || abmDF("EN_DATA_SRC") === lit("*")), lit("*"))
          .otherwise(csDF("CL_DATA_SRC")).as("CL_DATA_SRC"),
        coalesce(lobDF("PAYER_LOB"), lit("(unknown)")).as("PAYER_LOB"),
        csDF("FROM_DATE").as("YEAR_MO")
      )
      .agg(sum(csDF("CLAIMLINE_COUNT")).as("RECCNT"),
        sum(csDF("AMT_PAID")).as("AMT_PAID"),
        sum(csDF("AMT_BILLED")).as("AMT_BILLED"),
        sum(csDF("AMT_ALLOWED")).as("AMT_ALLOWED"),
        sum(csDF("AMT_COB")).as("AMT_COB"),
        sum(csDF("MEMBER_PAID")).as("COST_SHARE")
      )


    val etDF = enrollTotals
    val stDF = servTotals


    val outputDF = etDF.join(stDF, (etDF("DATA_SOURCE") === lit("*") || stDF("CL_DATA_SRC") === lit("*") || etDF("DATA_SOURCE") === stDF("CL_DATA_SRC")) &&
                                   etDF("YEAR_MO") === concat(year(stDF("YEAR_MO").cast(DataTypes.StringType)).cast(DataTypes.StringType),(month(stDF("YEAR_MO").cast(DataTypes.StringType)).cast(DataTypes.StringType)))  &&
                                   etDF("PAYER_LOB") === stDF("PAYER_LOB"), "right_outer"
                            )
                         .groupBy(coalesce(etDF("DATA_SOURCE"),stDF("CL_DATA_SRC")).as("DATA_SOURCE"), stDF("PAYER_LOB"), stDF("YEAR_MO"))
                        .agg(sum(etDF("MEMBER_MONTHS")).as("MEMBER_MONTHS"),
                          sum(stDF("RECCNT")).as("RECCNT"),
                          when(sum(etDF("MEMBER_MONTHS")) === lit(0), lit(0)).otherwise(sum(stDF("RECCNT"))/sum(etDF("MEMBER_MONTHS"))).as("REC_PER_MM"),
                          sum(stDF("AMT_BILLED")).as("AMT_BILLED"),
                          sum(stDF("AMT_ALLOWED")).as("AMT_ALLOWED"),
                          sum(stDF("COST_SHARE")).as("COST_SHARE"),
                          sum(stDF("AMT_COB")).as("AMT_COB"),
                          sum(stDF("AMT_PAID")).as("AMT_PAID"),
                          when(sum(stDF("AMT_ALLOWED")) === lit(0), lit(0)).otherwise(sum(stDF("AMT_ALLOWED")) - (sum(stDF("AMT_PAID")) + sum(stDF("AMT_COB")) + sum(stDF("COST_SHARE")) )/(sum(stDF("AMT_ALLOWED")))).as("REC_PER_MM"),
                          when(sum(etDF("MEMBER_MONTHS")) === lit(0), lit(0)).otherwise(sum(stDF("AMT_ALLOWED"))/sum(etDF("MEMBER_MONTHS"))).as("ALLOW_PMPM"),
                          when(sum(stDF("AMT_BILLED")) === lit(0), lit(0)).otherwise(sum(stDF("AMT_ALLOWED"))/sum(stDF("AMT_BILLED"))).as("ALLOWED_BILLED"),
                          when(sum(stDF("AMT_ALLOWED")) === lit(0), lit(0)).otherwise(sum(stDF("AMT_PAID"))/sum(stDF("AMT_ALLOWED"))).as("PAID_ALLOWED")
                        )


    return outputDF.persist()


  }


}
