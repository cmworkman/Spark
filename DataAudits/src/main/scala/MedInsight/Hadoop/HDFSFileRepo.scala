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
}

class HDFSFileRepo extends HDFS with ReferenceData with StagingData {}
