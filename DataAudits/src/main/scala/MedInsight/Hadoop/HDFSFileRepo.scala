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
