package MedInsight.Hadoop
import org.apache.spark.sql.{ SparkSession}

object App {

  def main(arg: Array[String]): Unit = {

    val miConfig = new MIConfig()
    val fileRepo = new HDFSFileRepo()

    val spark = SparkSession
      .builder()
      .appName("DataAudits")
      .getOrCreate()

    val Base_Staging_Claimline_DF = new HDFS_Reader(spark, fileRepo.STAGING_CLAIMLINE, new Staging_Claimline_Table().schema).read()
    val Dates_DF = new HDFS_Reader(spark, fileRepo.RFT_DATES, new Dates_Table().schema).read()

    val Audit_B_Claims_RowType_DF = new Populate_Audit_B_Claims_RowType(spark, miConfig, Base_Staging_Claimline_DF).populate()
    val Staging_Claimlin_DF       = new Populate_Staging_Claimline(spark, miConfig, Audit_B_Claims_RowType_DF, Base_Staging_Claimline_DF).populate()



  }

}
