package MedInsight.Hadoop

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{sum}

/**
  * Created by christopher.workman on 6/19/2018.
  */
class Populate_Claims_Summary_For_Enrollment(ss: SparkSession, miConfig: MIConfig, claimSummaryDF: DataFrame) {
  def populate() : DataFrame = {
    val clDF = claimSummaryDF

    val outputDF =  clDF.groupBy(clDF("CL_DATA_SRC"),clDF("FROMDATE").as("YEAR_MO"))
      .agg(sum(clDF("CLAIMLINE_COUNT")).as("RECCNT")
      )

    return outputDF
  }
}


