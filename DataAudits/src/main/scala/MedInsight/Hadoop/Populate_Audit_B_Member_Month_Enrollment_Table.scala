package MedInsight.Hadoop

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes}
/**
  * Created by christopher.workman on 6/20/2018.
  */
class Populate_Audit_B_Member_Month_Enrollment_Table (ss: SparkSession, miConfig: MIConfig, stagingEnrollmentDF: DataFrame, claimSummaryEnrollment: DataFrame, datesDF: DataFrame){
  def populate() : DataFrame = {
    val seDF  = stagingEnrollmentDF
    val cseDF = claimSummaryEnrollment

    // get the average claimline count by Data Source
    val avgClaimCountByDataSourceDF = cseDF.groupBy(cseDF("CL_DATA_SRC"))
                                           .agg(avg(cseDF("RECCNT")).as("AVERAGE"))

    // not only do we need the average claimline count over all of time by Data Source, we also need the claimline count by month (this is for two calcs below)
    val claimlinCountsWithOverTimeAverageByDataSource = avgClaimCountByDataSourceDF.join(cseDF,
                                    cseDF("CL_DATA_SRC") <=> avgClaimCountByDataSourceDF("CL_DATA_SRC")
                                    ,"inner")
                                    .select(cseDF("CL_DATA_SRC"),cseDF("YEAR_MO"),cseDF("RECCNT"),avgClaimCountByDataSourceDF("AVERAGE"))
                                    .where(cseDF("RECCNT")  > (lit(.5) * (avgClaimCountByDataSourceDF("AVERAGE"))) )


    // for each Data Source, find the max yearmo and subtract 4 years.  We're forming a window here.  Exclude months that have record counts below 50%
    // of our over-all-of-time per-month average for that Data Source
    val clcwDF = claimlinCountsWithOverTimeAverageByDataSource

    val dateRangeByDataSource = clcwDF.groupBy(clcwDF("CL_DATA_SRC"))
                  .agg(max(clcwDF("YEAR_MO")).as("MAX_YEARMO"), max(add_months(clcwDF("YEAR_MO"), -47)).as("MIN_YEARMO") )




    val outputDF = seDF.join(datesDF,datesDF("DATES") >= seDF("EFF_DATE") && datesDF("DATES") <= seDF("TERM_DATE") && datesDF("DAY_OF_MONTH") <=> 15 ,"inner")
                .join(dateRangeByDataSource,(seDF("EN_DATA_SRC") <=>  dateRangeByDataSource("CL_DATA_SRC") || seDF("EN_DATA_SRC") <=> "*" || dateRangeByDataSource("CL_DATA_SRC") <=> "*")
                  && (datesDF("YEAR_MO").cast(DataTypes.StringType) >= concat( year(dateRangeByDataSource("MIN_YEARMO")),month(dateRangeByDataSource("MIN_YEARMO")) ) &&
                      datesDF("YEAR_MO").cast(DataTypes.StringType) <= concat( year(dateRangeByDataSource("MAX_YEARMO")),month(dateRangeByDataSource("MAX_YEARMO")) ) )
                  ,"inner")
                    .groupBy(seDF("EN_DATA_SRC"),seDF("MEMBER_ID"),seDF("SUBSCRIBER_ID"),datesDF("YEAR_MO"),datesDF("FIRST_DATE_IN_MONTH"),datesDF("LAST_DATE_IN_MONTH"),coalesce(seDF("MEMBER_QUAL"),lit("''"))
                    )
                    .agg(max(seDF("MI_USER_DIM_01_")),max(seDF("MI_USER_DIM_02_")),max(seDF("MI_USER_DIM_03_")),max(seDF("MI_USER_DIM_04_")),max(seDF("MI_USER_DIM_05_")),
                      max(seDF("MI_USER_DIM_06_")),max(seDF("MI_USER_DIM_07_")),max(seDF("MI_USER_DIM_08_")),max(seDF("MI_USER_DIM_09_")),max(seDF("MI_USER_DIM_10_")),
                      max(seDF("PAYER_TYPE")),max(seDF("GRP_ID")),max(seDF("PCP_PROV"))
                      )





    //println("Sample output for  Populate_Claims_Summary_For_Enrollment" + dateRangeByDataSource.show(10))
    return outputDF
  }
}
