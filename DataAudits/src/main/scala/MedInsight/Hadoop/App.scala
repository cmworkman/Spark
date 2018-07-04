package MedInsight.Hadoop

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession, Dataset, SQLContext}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.{when,_}
import org.apache.spark.SparkConf

import org.apache.spark.sql.functions.{count, lit, sum, _}

object App {

  def main(arg: Array[String]): Unit = {

    val miConfig = new MIConfig()
    val fileRepo = new HDFSFileRepo()

    val sparkSession = SparkSession
      .builder()
      .appName("DataAudits")
/*      .config(
    new SparkConf().registerKryoClasses(
      Array(
        classOf[Staging_Claimline_Table],
        classOf[Staging_Enrollment_Table],
        classOf[Update_SubscriberID_For_ClaimSummary],

        classOf[Populate_Audit_B_Claims_RowType],
        classOf[Populate_Foundation_Table],
        classOf[Populate_Claims_Summary_For_Enrollment],
        classOf[Populate_Audit_B_Claims_RowType],

        classOf[Populate_Audit_B_Member_Month_Enrollment_Table],
        classOf[Populate_Staging_Claimline],




        classOf[scala.collection.mutable.WrappedArray.ofRef[_]],
        classOf[org.apache.spark.sql.types.StructType],
        classOf[Array[org.apache.spark.sql.types.StructType]],
        classOf[org.apache.spark.sql.types.StructField],
        classOf[Array[org.apache.spark.sql.types.StructField]],
        Class.forName("org.apache.spark.sql.types.StringType$"),
        Class.forName("org.apache.spark.sql.types.LongType$"),
        Class.forName("org.apache.spark.sql.types.BooleanType$"),
        Class.forName("org.apache.spark.sql.types.DoubleType$"),
        Class.forName("[[B"),
        classOf[org.apache.spark.sql.types.Metadata],
        classOf[org.apache.spark.sql.types.ArrayType],
        Class.forName("org.apache.spark.sql.execution.joins.UnsafeHashedRelation"),
        classOf[org.apache.spark.sql.catalyst.InternalRow],
        classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
        classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow],
        Class.forName("org.apache.spark.sql.execution.joins.LongHashedRelation"),
        Class.forName("org.apache.spark.sql.execution.joins.LongToUnsafeRowMap"),
        classOf[org.apache.spark.util.collection.BitSet],
        classOf[org.apache.spark.sql.types.DataType],
        classOf[Array[org.apache.spark.sql.types.DataType]],
        Class.forName("org.apache.spark.sql.types.NullType$"),
        Class.forName("org.apache.spark.sql.types.IntegerType$"),
        Class.forName("org.apache.spark.sql.types.TimestampType$"),
        Class.forName("org.apache.spark.sql.execution.datasources.FileFormatWriter$WriteTaskResult"),
        Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
        Class.forName("scala.collection.immutable.Set$EmptySet$"),
        Class.forName("scala.reflect.ClassTag$$anon$1"),
        Class.forName("java.lang.Class")
      )
    ))*/
/*      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // use this if you need to increment Kryo buffer size. Default 64k
      .config("spark.kryoserializer.buffer", "1024k")
      // use this if you need to increment Kryo buffer max size. Default 64m
      .config("spark.kryoserializer.buffer.max", "1024m")
      /*
      * Use this if you need to register all Kryo required classes.
      * If it is false, you do not need register any class for Kryo, but it will increase your data size when the data is serializing.
      */
      .config("spark.kryo.registrationRequired", "true")*/
      .getOrCreate()

    import sparkSession.implicits._

    val r = scala.util.Random


    //load base staging rdds
    val Base_Staging_Claimline_DF =  new HDFS_Reader(sparkSession, fileRepo.STAGING_CLAIMLINE, new Staging_Claimline_Table().schema  ).read()
/*    val Base_Staging_Premium_DF =    new HDFS_Reader(sparkSession, fileRepo.STAGING_PREMIUM,    new Staging_Premium_Table().schema).read()*/
    val Base_Staging_Enrollment_DF = new HDFS_Reader(sparkSession, fileRepo.STAGING_ENROLLMENT, new Staging_Enrollment_Table().schema ).read()

/*    val Base_Staging_Group_DF =      new HDFS_Reader(sparkSession, fileRepo.STAGING_GROUP,      new Staging_Group_Table().schema).read()*/
/*    val Base_Staging_Provider_DF =   new HDFS_Reader(sparkSession, fileRepo.STAGING_PROVIDER,   new Staging_Provider_Table().schema).read()
    val Base_Staging_Capitation_DF = new HDFS_Reader(sparkSession, fileRepo.STAGING_CAPITATION, new Staging_Capitation_Table().schema).read()*/
    val Base_Staging_Member_DF     = new HDFS_Reader(sparkSession, fileRepo.STAGING_MEMBER, new Staging_Member_Table().schema   ).read()



/*    Base_Staging_Claimline_DF.coalesce(1).write.parquet("wasb:///HDFS/StagingData/STAGING_CLAIMLINE_P1")*/
      //.parquet("wasb:///HDFS/StagingData/STAGING_CLAIMLINE_P")
/*    Base_Staging_Enrollment_DF.coalesce(1).write.parquet("wasb:///HDFS/StagingData/STAGING_ENROLLMENT_P1")
    Base_Staging_Member_DF.coalesce(1).write.parquet("wasb:///HDFS/StagingData/STAGING_MEMBER_P1")*/



    //load reference datasets
    val Dates_Pre =      new HDFS_Reader(sparkSession, fileRepo.RFT_DATES,    new Dates_Table().schema).read()
/*    Dates_Pre.toDF().write.option("header","true").csv("wasb://workcluster2@workmanstorage.blob.core.windows.net/RefDataSets/output2.csv")*/
    val Dates_DF = Dates_Pre.select("DATES","YEARS","MONTHS","YEAR_MO", "DAY_OF_MONTH", "FIRST_DATE_IN_MONTH","LAST_DATE_IN_MONTH")
           .where( (Dates_Pre("DAY_OF_MONTH") === lit(1) || Dates_Pre("DAY_OF_MONTH") === lit(15) || Dates_Pre("DAY_OF_MONTH") > lit(27) )
             && (Dates_Pre("YEARS") > lit(2010) && Dates_Pre("YEARS") < lit(2019)) || Dates_Pre("YEARS") === lit(1900) || Dates_Pre("YEARS") === lit(2099)  )
    val Dimensions_DF = new HDFS_Reader(sparkSession, fileRepo.MI_DIMENSIONS, new Dimensions_Table().schema).read()
    val Payer_LOB_DF  = new HDFS_Reader(sparkSession, fileRepo.RFT_PAYER_LOB, new RFT_PAYER_LOB_Table().schema).read()
    val Payer_Type_DF = new HDFS_Reader(sparkSession, fileRepo.RFT_PAYER_TYPE, new RFT_PAYER_TYPE_Table().schema).read()



    var dig = r.nextInt(2000).toString
    //let's calculate aggregates for each claim id (claim type, src, member id, month, dollar totals)
    val Audit_B_Claims_RowType_DF = new Populate_Audit_B_Claims_RowType(sparkSession, miConfig, Base_Staging_Claimline_DF).populate()

/*    Audit_B_Claims_RowType_DF.limit(20).toDF().write.option("header","true").csv("wasb://workcluster2@workmanstorage.blob.core.windows.net/Results/"+dig+".csv")*/
    //basiscally a raw claimline pull
    val Staging_Claimline_DF      = new Populate_Staging_Claimline(sparkSession, miConfig, Audit_B_Claims_RowType_DF, Base_Staging_Claimline_DF).populate()
    println("Staging Claimline Cnt:" + Staging_Claimline_DF.show(10) )
    println("Audit B Claims RowType Cnt:" + Audit_B_Claims_RowType_DF.show(10) )

    //summary by claim id (very similar to Audit_B_Claims_RowType)
    val Claims_Summary_DF         = new Populate_Claims_Summary(sparkSession, miConfig, Staging_Claimline_DF).populate()
    println("Claim Summary:" + Claims_Summary_DF.show(100) )
    //claimline count by yearmo and datasource (does datasrc actually vary by client?)
    val Claims_Summary_Enr_DF     = new Populate_Claims_Summary_For_Enrollment(sparkSession, miConfig, Claims_Summary_DF).populate()


    println("CSENR show:" + Claims_Summary_Enr_DF.show(100) )

    val Audit_B_Member_Month_Enrollment_DF = new Populate_Audit_B_Member_Month_Enrollment_Table(sparkSession, miConfig, Base_Staging_Enrollment_DF, Claims_Summary_Enr_DF, Dates_DF, Base_Staging_Member_DF).populate()

    println("Audit B Member_Month Cnt:" + Audit_B_Member_Month_Enrollment_DF.show(10) )

    dig = r.nextInt(1000).toString
    val Claims_Summary_W_SID_DF  = new Update_SubscriberID_For_ClaimSummary(sparkSession, miConfig, Audit_B_Member_Month_Enrollment_DF, Claims_Summary_DF).populate()


    val Month_Count_By_DataSource_In_Audit = new Populate_Month_Count_By_DataSource_In_Audit(sparkSession, miConfig, Audit_B_Member_Month_Enrollment_DF).populate()
    val Audit_Member_Years = new Audit_Member_Years_Populate(sparkSession, miConfig, Audit_B_Member_Month_Enrollment_DF).populate()
    val Audit_Claim_Lines  = new Audit_Claim_Lines_Populate(sparkSession, miConfig, Audit_B_Claims_RowType_DF).populate()
    val Audit_Bill_Provider = new Audit_Bill_Provider_Populate(sparkSession, miConfig, Staging_Claimline_DF).populate()
    val Audit_Attending_Provider = new Audit_Attending_Provider_Populate(sparkSession, miConfig, Staging_Claimline_DF).populate()

    val Foundation_Table = new Populate_Foundation_Table(sparkSession, miConfig, Payer_LOB_DF, Payer_Type_DF, Audit_B_Member_Month_Enrollment_DF, Claims_Summary_DF ).populate()
    dig = r.nextInt(1000).toString
    println("Foundation Table Cnt:" + Foundation_Table.show(100) )
  /*  Foundation_Table.limit(1000).toDF().write.option("header","true").csv("wasb://workcluster2@workmanstorage.blob.core.windows.net/Results/"+dig+".csv")*/
  }

}
