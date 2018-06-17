package MedInsight.Hadoop

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by christopher.workman on 6/17/2018.
  */
class Read_Staging_Claimline(ss: SparkSession){
  def read(): DataFrame = {

    return ss.read.format("csv")
      .option("sep", "|")
      .option("header", true)
      .schema(new Staging_Claimline_Table().schema)
      .load("wasb:///HDFS/dbo.STAGING_CLAIMLINE.bcp")
  }
}
