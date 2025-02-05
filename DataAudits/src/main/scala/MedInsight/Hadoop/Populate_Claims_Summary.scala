package MedInsight.Hadoop

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
/**
  * Created by christopher.workman on 6/17/2018.
  */
class Populate_Claims_Summary(ss: SparkSession, miConfig: MIConfig, stagingClaimlineDF: DataFrame) {
  def populate() : DataFrame = {
    val clDF = stagingClaimlineDF

    val outputDF = clDF.groupBy(clDF("CL_DATA_SRC"), clDF("CLAIM_ID"), clDF("SVYEARMO"), clDF("ROW_TYPE"), clDF("SV_STAT"), clDF("DIS_STAT"),
                              concat(substring(clDF("FROM_DATE").cast(DataTypes.StringType),1,4),substring(clDF("FROM_DATE").cast(DataTypes.StringType),6,2)).as("FROMDATE"),
                              concat(substring(clDF("ADM_DATE").cast(DataTypes.StringType),1,4),substring(clDF("ADM_DATE").cast(DataTypes.StringType),6,2)).as("ADMITDATE"),
                              concat(substring(clDF("TO_DATE").cast(DataTypes.StringType),1,4),substring(clDF("TO_DATE").cast(DataTypes.StringType),6,2)).as("TODATE"),
                              concat(substring(clDF("DIS_DATE").cast(DataTypes.StringType),1,4),substring(clDF("DIS_DATE").cast(DataTypes.StringType),6,2)).as("DISCHARGEDATE"),
                              clDF("MEMBER_ID"), clDF("BILL_PROV"),
                              coalesce(clDF("CLAIM_IN_NETWORK"), lit("")).as("CLAIM_IN_NETWORK"), coalesce(clDF("MEMBER_QUAL"), lit("")).as("MEMBER_QUAL"))
                      .agg(sum(clDF("SV_UNITS")).as("SV_UNITS"),sum(clDF("AMT_BILLED")).as("AMT_BILLED"),sum(clDF("AMT_ALLOWED")).as("AMT_ALLOWED"),
                           sum(clDF("AMT_PAID")).as("AMT_PAID"),sum(clDF("RX_DAYS_SUPPLY")).as("RX_DAYS_SUPPLY"),sum(clDF("AMT_COB")).as("AMT_COB"),
                           sum(clDF("AMT_COPAY") + clDF("AMT_COINS") + clDF("AMT_DEDUCT")).as("MEMBER_PAID"),
                           count(clDF("*")).as("CLAIMLINE_COUNT")
                           )

    //println("Sample output for  Populate_Claims_Summary" + outputDF.show(10))
    return outputDF
  }
}
