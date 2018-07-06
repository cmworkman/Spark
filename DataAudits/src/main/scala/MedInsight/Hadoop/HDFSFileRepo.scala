package MedInsight.Hadoop

/**
  * Created by christopher.workman on 6/17/2018.
  */

trait HDFS {
  def baseLoc = "wasb:///"
  //def baseLoc = "wasb://workcluster2@workmanstorage.blob.core.windows.net/"
}

trait ReferenceData extends HDFS {
  def rloc              = baseLoc + "RefDataSets/"
  def RFT_DATES         = rloc + "RFT_DATES.bcp"
  def RFT_DIS_STAT      = rloc + "RFT_DIS_STAT.bcp"
  def RFT_ICD9_DIAG     = rloc + "RFT_ICD9_DIAG.bcp"
  def RFT_ICD9_PROC     = rloc + "RFT_ICD9_PROC.bcp"
  def RFT_ICD10_DIAG    = rloc + "RFT_ICD10_DIAG.bcp"
  def RFT_ICD10_PROC    = rloc + "RFT_ICD10_PROC.bcp"
  def RFT_PAYER_LOB     = rloc + "PAYER_LOB.bcp"
  def RFT_PAYER_TYPE    = rloc + "PAYER_TYPE.bcp"
  def RFT_PROC_CODE     = rloc + "RFT_PROC_CODE.bcp"
  def RFT_REV_CODE      = rloc + "RFT_REV_CODE.bcp"


}

trait StagingData extends HDFS {
  def sloc                  = baseLoc + "StagingData/"
/*  def STAGING_CLAIMLINE     = sloc + "STAGING_CLAIMLINE_2.parq/part-00000-abc72c3b-f8b0-42bf-b798-ac4234fdf8e7-c000.snappy.parquet"*/
//  def STAGING_CLAIMLINE     = sloc + "STAGING_CLAIMLINE_FULL_TEST_1.parq"
/*  def STAGING_CLAIMLINE     = sloc + "STAGING_CLAIMLINE_FULL_1.bcp"*/
/*  def STAGING_CLAIMLINE     = sloc + "test10.bcp"*/
  def STAGING_CLAIMLINE     = sloc + "STAGING_CLAIMLINE_FULL_ENCODED.bcp"
  //def STAGING_CLAIMLINE     = sloc + "STAGING_CLAIMLINE.bcp"
  def STAGING_ENROLLMENT    = sloc + "STAGING_ENROLLMENT.parq/part-00000-c62491d8-f224-4e63-9a45-08efd8d5005f-c000.snappy.parquet"
/*  def STAGING_ENROLLMENT    = sloc + "STAGING_ENROLLMENT.bcp"*/
  def STAGING_PREMIUM       = sloc + "STAGING_PREMIUM.bcp"
  def STAGING_GROUP         = sloc + "STAGING_GROUP.bcp"
  def STAGING_CAPITATION    = sloc + "STAGING_CAPITATION.bcp"
  def STAGING_MEMBER        = sloc + "STAGING_MEMBER.parq/part-00000-133b848f-2fb8-4ee3-bfe1-8a794595fa5c-c000.snappy.parquet"
/*  def STAGING_MEMBER        = sloc + "STAGING_MEMBER.bcp"*/
  def STAGING_PROVIDER      = sloc + "STAGING_PROVIDER.bcp"
  def MI_DIMENSIONS         = sloc + "MI_DIMENSIONS.bcp"
}

class HDFSFileRepo extends HDFS with ReferenceData with StagingData {}
