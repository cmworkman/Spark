package MedInsight.Hadoop

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{count, lit, sum, _}

object App {

  def main(arg: Array[String]): Unit = {

    val miConfig = new MIConfig()
    val fileRepo = new HDFSFileRepo()

/*    val df = spark.read.json("json")*/


    val sparkSession = SparkSession
      .builder()
      .appName("DataAudits")
      .getOrCreate()




    //load base staging rdds
//    val Base_Staging_Claimline_DF =  new HDFS_Reader_Parq(sparkSession, fileRepo.STAGING_CLAIMLINE, new Staging_Claimline_Table().schema  ).read()
//    val Base_Staging_Member_DF     = new HDFS_Reader_Parq(sparkSession, fileRepo.STAGING_MEMBER, new Staging_Member_Table().schema).read().repartition(50)
//    val Base_Staging_Member_DF     = new HDFS_Reader_Parq(sparkSession, fileRepo.STAGING_MEMBER, new Staging_Member_Table().schema).read().repartition(50)

    val Base_Staging_Claimline_DF =  new HDFS_Reader(sparkSession, fileRepo.STAGING_CLAIMLINE, new Staging_Claimline_Table().schema  ).read()
    val Base_Staging_Premium_DF =    new HDFS_Reader(sparkSession, fileRepo.STAGING_PREMIUM,    new Staging_Premium_Table().schema).read()
    val Base_Staging_Enrollment_DF = new HDFS_Reader_Parq(sparkSession, fileRepo.STAGING_ENROLLMENT, new Staging_Enrollment_Table().schema ).read()

    val Base_Staging_Group_DF =      new HDFS_Reader(sparkSession, fileRepo.STAGING_GROUP,      new Staging_Group_Table().schema).read()
    val Base_Staging_Provider_DF =   new HDFS_Reader(sparkSession, fileRepo.STAGING_PROVIDER,   new Staging_Provider_Table().schema).read()
    val Base_Staging_Capitation_DF = new HDFS_Reader(sparkSession, fileRepo.STAGING_CAPITATION, new Staging_Capitation_Table().schema).read()
    val Base_Staging_Member_DF     = new HDFS_Reader(sparkSession, fileRepo.STAGING_MEMBER, new Staging_Member_Table().schema   ).read()




    //load reference datasets
    val Dates_Pre =      new HDFS_Reader(sparkSession, fileRepo.RFT_DATES,    new Dates_Table().schema).read()
    val Dates_DF = Dates_Pre.select("DATES","YEARS","MONTHS","YEAR_MO", "DAY_OF_MONTH", "FIRST_DATE_IN_MONTH","LAST_DATE_IN_MONTH")
           .where( (Dates_Pre("DAY_OF_MONTH") === lit(1) || Dates_Pre("DAY_OF_MONTH") === lit(15) || Dates_Pre("DAY_OF_MONTH") > lit(27) )
             && (Dates_Pre("YEARS") > lit(2010) && Dates_Pre("YEARS") < lit(2019)) || Dates_Pre("YEARS") === lit(1900) || Dates_Pre("YEARS") === lit(2099)  )
    val Dimensions_DF = new HDFS_Reader(sparkSession, fileRepo.MI_DIMENSIONS, new Dimensions_Table().schema).read()
    val Payer_LOB_DF  = new HDFS_Reader(sparkSession, fileRepo.RFT_PAYER_LOB, new RFT_PAYER_LOB_Table().schema).read()
    val Payer_Type_DF = new HDFS_Reader(sparkSession, fileRepo.RFT_PAYER_TYPE, new RFT_PAYER_TYPE_Table().schema).read()

    //let's calculate aggregates for each claim id (claim type, src, member id, month, dollar totals)
    val Audit_B_Claims_RowType_DF = new Populate_Audit_B_Claims_RowType(sparkSession, miConfig, Base_Staging_Claimline_DF).populate()

    //basiscally a raw claimline pull
    val Staging_Claimline_DF      = new Populate_Staging_Claimline(sparkSession, miConfig, Audit_B_Claims_RowType_DF, Base_Staging_Claimline_DF).populate()
    //Staging_Claimline_DF.persist()

    //summary by claim id (very similar to Audit_B_Claims_RowType)
    val Claims_Summary_DF         = new Populate_Claims_Summary(sparkSession, miConfig, Staging_Claimline_DF).populate()
    //claimline count by yearmo and datasource (does datasrc actually vary by client?)
    val Claims_Summary_Enr_DF     = new Populate_Claims_Summary_For_Enrollment(sparkSession, miConfig, Claims_Summary_DF).populate()
    val Audit_B_Member_Month_Enrollment_DF = new Populate_Audit_B_Member_Month_Enrollment_Table(sparkSession, miConfig, Base_Staging_Enrollment_DF, Claims_Summary_Enr_DF, Dates_DF, Base_Staging_Member_DF).populate()

/*    val Claims_Summary_W_SID_DF  = new Update_SubscriberID_For_ClaimSummary(sparkSession, miConfig, Audit_B_Member_Month_Enrollment_DF, Claims_Summary_DF).populate()*/


    val Month_Count_By_DataSource_In_Audit = new Populate_Month_Count_By_DataSource_In_Audit(sparkSession, miConfig, Audit_B_Member_Month_Enrollment_DF).populate()
    val Audit_Member_Years = new Audit_Member_Years_Populate(sparkSession, miConfig, Audit_B_Member_Month_Enrollment_DF).populate()
    val Audit_Claim_Lines  = new Audit_Claim_Lines_Populate(sparkSession, miConfig, Audit_B_Claims_RowType_DF).populate()
    val Audit_Bill_Provider = new Audit_Bill_Provider_Populate(sparkSession, miConfig, Staging_Claimline_DF).populate()
    val Audit_Attending_Provider = new Audit_Attending_Provider_Populate(sparkSession, miConfig, Staging_Claimline_DF).populate()

    val Foundation_Table = new Populate_Foundation_Table(sparkSession, miConfig, Payer_LOB_DF, Payer_Type_DF, Audit_B_Member_Month_Enrollment_DF, Claims_Summary_DF ).populate()
    println("Foundation Table total: " + Foundation_Table.show(10))
  }

}
