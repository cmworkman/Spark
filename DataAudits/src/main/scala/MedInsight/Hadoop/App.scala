package MedInsight.Hadoop
import org.apache.spark.sql.{DataFrame, SparkSession}

object App {

  def main(arg: Array[String]): Unit = {

    val miConfig = new MIConfig()

    val spark = SparkSession
      .builder()
      .appName("")
      .getOrCreate()

    val Base_Staging_Claimline_DF = new Read_Staging_Claimline(spark).read()
    val Audit_B_Claims_RowType_DF = new Populate_Audit_B_Claims_RowType(spark, miConfig, Base_Staging_Claimline_DF).populate()
    val Staging_Claimlin_DF       = new Populate_Staging_Claimline(spark, miConfig, Audit_B_Claims_RowType_DF, Base_Staging_Claimline_DF).populate()



  }

}
