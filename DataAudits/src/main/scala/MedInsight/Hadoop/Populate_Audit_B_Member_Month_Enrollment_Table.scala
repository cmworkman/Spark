package MedInsight.Hadoop

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{coalesce, _}
import org.apache.spark.sql.types.DataTypes
/**
  * Created by christopher.workman on 6/20/2018.
  */
class Populate_Audit_B_Member_Month_Enrollment_Table (ss: SparkSession, miConfig: MIConfig, stagingEnrollmentDF: DataFrame, claimSummaryEnrollmentDF: DataFrame, datesDF: DataFrame, stagingMemberDF : DataFrame){
  def populate() : DataFrame = {
    val seDF  = stagingEnrollmentDF
    val cseDF = claimSummaryEnrollmentDF
    val smDF  = stagingMemberDF

    // get the average claimline count by Data Source
    val avgClaimCountByDataSourceDF = cseDF.groupBy(cseDF("CL_DATA_SRC"))
                                           .agg(avg(cseDF("RECCNT")).as("AVERAGE"))

    // not only do we need the average claimline count over all of time by Data Source, we also need the claimline count by month (this is for two calcs below)
    val claimlinCountsWithOverTimeAverageByDataSource = avgClaimCountByDataSourceDF.join(cseDF,
                                    cseDF("CL_DATA_SRC") === avgClaimCountByDataSourceDF("CL_DATA_SRC")
                                    ,"inner")
                                    .select(cseDF("CL_DATA_SRC"),cseDF("YEAR_MO"),cseDF("RECCNT"),avgClaimCountByDataSourceDF("AVERAGE"))
                                    .where(cseDF("RECCNT")  > (lit(.5) * (avgClaimCountByDataSourceDF("AVERAGE"))) )


    // for each Data Source, find the max yearmo and subtract 4 years.  We're forming a window here.  Exclude months that have record counts below 50%
    // of our over-all-of-time per-month average for that Data Source
    val clcwDF = claimlinCountsWithOverTimeAverageByDataSource

    val dateRangeByDataSource = clcwDF.groupBy(clcwDF("CL_DATA_SRC"))
                  .agg(max(clcwDF("YEAR_MO")).as("MAX_YEARMO"),
                    max(((clcwDF("YEAR_MO")).cast(DataTypes.LongType) - lit(400))).as("MIN_YEARMO")
                  )


    // get dimensionality for every member's member month (jesus christ)
    val outputPreMemberDF =
/*                        seDF.join(datesDF,datesDF("DATES") >= seDF("EFF_DATE") && datesDF("DATES") <= seDF("TERM_DATE") && datesDF("DAY_OF_MONTH") === lit(15) ,"inner")*/
/*                        seDF.join(datesDF,datesDF("YEARMO") ===  concat(substring(seDF("EFF_DATE").cast(DataTypes.StringType),1,4),substring(seDF("EFF_DATE").cast(DataTypes.StringType),6,2)) && datesDF("DAY_OF_MONTH") === lit(15) ,"inner")
                       .join(dateRangeByDataSource,(seDF("EN_DATA_SRC") ===  dateRangeByDataSource("CL_DATA_SRC") || seDF("EN_DATA_SRC") === "*" || dateRangeByDataSource("CL_DATA_SRC") === "*")
                          && (datesDF("YEAR_MO").cast(DataTypes.StringType) >= dateRangeByDataSource("MIN_YEARMO").cast(DataTypes.StringType) ) &&
                          datesDF("YEAR_MO").cast(DataTypes.StringType) <= dateRangeByDataSource("MAX_YEARMO").cast(DataTypes.StringType)
                        ,"inner")*/
                    seDF.
/*                      join(
                      datesDF,datesDF("YEAR_MO") ===  concat(substring(seDF("EFF_DATE").cast(DataTypes.StringType),1,4),substring(seDF("EFF_DATE").cast(DataTypes.StringType),6,2)) && datesDF("DAY_OF_MONTH") === lit(15) ,"inner")*/
                      join(dateRangeByDataSource,(seDF("EN_DATA_SRC") ===  dateRangeByDataSource("CL_DATA_SRC") || seDF("EN_DATA_SRC") === "*" || dateRangeByDataSource("CL_DATA_SRC") === "*")
                        ,"inner")
                       .groupBy(seDF("EN_DATA_SRC"),seDF("MEMBER_ID"),seDF("EFF_DATE"),seDF("SUBSCRIBER_ID"),
/*                         datesDF("YEAR_MO"),*/
                         concat(substring(seDF("EFF_DATE").cast(DataTypes.StringType),1,4),substring(seDF("EFF_DATE").cast(DataTypes.StringType),6,2)).as("YEAR_MO"),
                         concat(substring(seDF("EFF_DATE").cast(DataTypes.StringType),1,4),lit("-"),substring(seDF("EFF_DATE").cast(DataTypes.StringType),6,2),lit("-01")).as("FIRST_DATE_IN_MONTH"),
/*                         datesDF("FIRST_DATE_IN_MONTH"),*/
/*                         datesDF("LAST_DATE_IN_MONTH"),*/
                         coalesce(seDF("MEMBER_QUAL"),lit("")).as("MEMBER_QUAL")
                        )
                       .agg(max(seDF("MI_USER_DIM_01_")).as("MI_USER_DIM_01_"),max(seDF("MI_USER_DIM_02_")).as("MI_USER_DIM_02_"),
                         max(seDF("MI_USER_DIM_03_")).as("MI_USER_DIM_03_"),max(seDF("MI_USER_DIM_04_")).as("MI_USER_DIM_04_"),
                         max(seDF("MI_USER_DIM_05_")).as("MI_USER_DIM_05_"), max(seDF("MI_USER_DIM_06_")).as("MI_USER_DIM_06_"),
                         max(seDF("MI_USER_DIM_07_")).as("MI_USER_DIM_07_"),max(seDF("MI_USER_DIM_08_")).as("MI_USER_DIM_08_"),
                         max(seDF("MI_USER_DIM_09_")).as("MI_USER_DIM_09_"),max(seDF("MI_USER_DIM_10_")).as("MI_USER_DIM_10_"),
                         max(seDF("PAYER_TYPE")).as("PAYER_TYPE"),max(seDF("GRP_ID")).as("GRP_ID"),max(seDF("PCP_PROV")).as("PCP_PROV")
                       )

    return outputPreMemberDF
/*    val opmDF = outputPreMemberDF
    val outputDF = opmDF.join(smDF, smDF("MEMBER_ID") === opmDF("MEMBER_ID") &&
                          (coalesce(smDF("MEMBER_QUAL"), lit("")) === coalesce(opmDF("MEMBER_QUAL"), lit(""))) &&
                          (opmDF("EFF_DATE") >= coalesce(smDF("MEM_START_DATE"), to_date(lit("1900-01-01"))) && opmDF("EFF_DATE") <= coalesce(smDF("MEM_START_DATE"), to_date(lit("2099-12-31"))) ) &&
                          (smDF("MEM_DATA_SRC") === lit("'*'") || opmDF("EN_DATA_SRC") === lit("'*'") || (smDF("MEM_DATA_SRC") === opmDF("EN_DATA_SRC"))), "left_outer"
                        )
        .select(smDF("MEM_GENDER"), smDF("MEM_DOB"), smDF("MEM_LNAME"), smDF("MEM_FNAME"),
                opmDF("EN_DATA_SRC"),
                opmDF("MEMBER_ID"),
                opmDF("SUBSCRIBER_ID"),
                opmDF("YEAR_MO"),
                opmDF("FIRST_DATE_IN_MONTH"),
                opmDF("LAST_DATE_IN_MONTH"),
                opmDF("MEMBER_QUAL"),
                opmDF("MI_USER_DIM_01_"),
                opmDF("MI_USER_DIM_02_"),
                opmDF("MI_USER_DIM_03_"),
                opmDF("MI_USER_DIM_04_"),
                opmDF("MI_USER_DIM_05_"),
                opmDF("MI_USER_DIM_06_"),
                opmDF("MI_USER_DIM_07_"),
                opmDF("MI_USER_DIM_08_"),
                opmDF("MI_USER_DIM_09_"),
                opmDF("MI_USER_DIM_10_"),
                opmDF("PAYER_TYPE"),
                opmDF("GRP_ID"),
                opmDF("PCP_PROV")
        )
    return outputDF*/
  }
}
