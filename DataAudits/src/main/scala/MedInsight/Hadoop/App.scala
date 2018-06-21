package MedInsight.Hadoop
import org.apache.spark.sql.{ SparkSession}

object App {

  def main(arg: Array[String]): Unit = {

    val miConfig = new MIConfig()
    val fileRepo = new HDFSFileRepo()

    val sparkSession = SparkSession
      .builder()
      .appName("DataAudits")
      .getOrCreate()

    //load base staging rdds
    val Base_Staging_Claimline_DF =  new HDFS_Reader(sparkSession, fileRepo.STAGING_CLAIMLINE,  new Staging_Claimline_Table().schema).read()
    val Base_Staging_Premium_DF =    new HDFS_Reader(sparkSession, fileRepo.STAGING_PREMIUM,    new Staging_Premium_Table().schema).read()
    val Base_Staging_Enrollment_DF = new HDFS_Reader(sparkSession, fileRepo.STAGING_ENROLLMENT, new Staging_Enrollment_Table().schema).read()
    val Base_Staging_Group_DF =      new HDFS_Reader(sparkSession, fileRepo.STAGING_GROUP,      new Staging_Group_Table().schema).read()
    val Base_Staging_Provider_DF =   new HDFS_Reader(sparkSession, fileRepo.STAGING_PROVIDER,   new Staging_Provider_Table().schema).read()
    val Base_Staging_Capitation_DF = new HDFS_Reader(sparkSession, fileRepo.STAGING_CAPITATION, new Staging_Capitation_Table().schema).read()




    //load reference datasets
    val Dates_DF =      new HDFS_Reader(sparkSession, fileRepo.RFT_DATES,    new Dates_Table().schema).read()
    val Dimensions_DF = new HDFS_Reader(sparkSession, fileRepo.MI_DIMENSIONS, new Dimensions_Table().schema).read()

    //let's calculate aggregates for each claim id (claim type, src, member id, month, dollar totals)
    val Audit_B_Claims_RowType_DF = new Populate_Audit_B_Claims_RowType(sparkSession, miConfig, Base_Staging_Claimline_DF).populate()
    //basiscally a raw claimline pull
    val Staging_Claimline_DF      = new Populate_Staging_Claimline(sparkSession, miConfig, Audit_B_Claims_RowType_DF, Base_Staging_Claimline_DF).populate()


    //summary by claim id (very similar to Audit_B_Claims_RowType)
    val Claims_Summary_DF         = new Populate_Claims_Summary(sparkSession, miConfig, Staging_Claimline_DF).populate()
    //claimline count by yearmo and datasource (does datasrc actually vary by client?)
    val Claims_Summary_Enr_DF     = new Populate_Claims_Summary_For_Enrollment(sparkSession, miConfig, Claims_Summary_DF).populate()


    val Audit_B_Member_Month_Enrollment_DF = new Populate_Audit_B_Member_Month_Enrollment_Table(sparkSession, miConfig, Base_Staging_Enrollment_DF, Claims_Summary_Enr_DF, Dates_DF).populate()

  }

}
