package MedInsight.Hadoop

import java.sql.Struct

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by christopher.workman on 6/17/2018.
  */
class HDFS_Reader(ss: SparkSession, filePath: String, schema: StructType){
  def read(): DataFrame = {

    return ss.read.format("csv")
      .option("sep", "|")
      .option("header", false)
      .schema(schema)
      .load(filePath)
  }
}

class HDFS_Reader_Parq(ss: SparkSession, filePath: String, schemaa: StructType){
  def read(): DataFrame = {

    return ss.read.parquet(filePath)
  }
}
