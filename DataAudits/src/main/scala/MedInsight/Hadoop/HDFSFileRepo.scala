package MedInsight.Hadoop

/**
  * Created by christopher.workman on 6/17/2018.
  */

trait HDFS {
  def baseLoc = "wasb:///HDFS/"
}

trait ReferenceData extends HDFS {
  def rloc              = baseLoc + "ReferenceDatasets/"
  def RFT_DATES         = rloc + "RFT_DATES.bcp"
  def RFT_DIS_STAT      = rloc + "RFT_DIS_STAT.bcp"
  def RFT_ICD9_DIAG     = rloc + "RFT_ICD9_DIAG.bcp"
  def RFT_ICD9_PROC     = rloc + "RFT_ICD9_PROC.bcp"
  def RFT_ICD10_DIAG    = rloc + "RFT_ICD10_DIAG.bcp"
  def RFT_ICD10_PROC    = rloc + "RFT_ICD10_PROC.bcp"
  def RFT_PAYER_LOB     = rloc + "RFT_PAYER_LOB.bcp"
  def RFT_PAYER_TYPE    = rloc + "RFT_PAYER_TYPE.bcp"
  def RFT_PROC_CODE     = rloc + "RFT_PROC_CODE.bcp"
  def RFT_REV_CODE      = rloc + "RFT_REV_CODE.bcp"


}

trait StagingData extends HDFS {
  def sloc                  = baseLoc + "StagingData/"
  def STAGING_CLAIMLINE     = sloc + "STAGING_CLAIMLINE.bcp"
  def STAGING_ENROLLMENT    = sloc + "STAGING_ENROLLMENT.bcp"
  def STAGING_PREMIUM       = sloc + "STAGING_PREMIUM.bcp"
  def STAGING_GROUP         = sloc + "STAGING_GROUP.bcp"
  def STAGING_CAPITATION    = sloc + "STAGING_CAPITATION.bcp"
  def STAGING_MEMBER        = sloc + "STAGING_MEMBER.bcp"
  def STAGING_PROVIDER      = sloc + "STAGING_PROVIDER.bcp"
  def MI_DIMENSIONS         = sloc + "MI_DIMENSIONS.bcp"
}

class HDFSFileRepo extends HDFS with ReferenceData with StagingData {}
