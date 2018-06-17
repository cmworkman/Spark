package MedInsight.Hadoop
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions.{coalesce, isnan, lit, when, max, min, sum, count}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrameNaFunctions

object App {

  def main(arg: Array[String]): Unit = {

    val miConfig = new MIConfig()

    val spark = SparkSession
      .builder()
      .appName("")
      .getOrCreate()

    val clDF = spark.read.format("csv")
      .option("sep", "|")
      .option("header", true)
      .schema(new Staging_Claimline_Table().schema)
      .load("wasb:///HDFS/dbo.STAGING_CLAIMLINE.bcp")



    val bcDF = clDF.select(

      coalesce(clDF("CL_DATA_SRC"),lit("")).as("DATA_SOURCE"),
      when(clDF("FORM_TYPE") === "U",
        (when(
          ((clDF("MS_DRG").isNotNull) && (clDF("MS_DRG") !== "") && (clDF("MS_DRG") !== "0")) ||
            clDF("POS").isin(Seq("21", "25", "51", "61") : _*) ||
            clDF("UB_BILL_TYPE").isin(Seq("11", "12", "17", "18", "21", "22", "27", "28"): _*), "1HIP").otherwise("2HOP"))
      ).
        when(clDF("FORM_TYPE") === "H", "3PHY").
        when(clDF("FORM_TYPE") === "D", "4RX").
        when(clDF("FORM_TYPE") === "A", "5DEN").
        when(clDF("FORM_TYPE") === "V", "6VIS").
        when(clDF("FORM_TYPE") === "L", "7LAB").
        otherwise("ERR").as("ROW_TYPE"),
      clDF("CLAIM_ID"),
      clDF("AMT_BILLED"),
      clDF("AMT_DEDUCT"),
      clDF("AMT_ALLOWED"),
      clDF("AMT_COPAY"),
      clDF("AMT_PAID"),
      clDF("AMT_COINS"),
      clDF("AMT_COB"),
      clDF("MEMBER_ID"),
      clDF("MEMBER_QUAL"),
      when(clDF("FROM_DATE").isin(miConfig.proxyNullDates : _*) || clDF("FROM_DATE").isin(miConfig.proxyOpenDates: _*) , "-NULL-").otherwise(coalesce(clDF("FROM_DATE"),clDF("DIS_DATE"))).as("SVYEARMO")
    )

    val output = bcDF.groupBy(bcDF("DATA_SOURCE"),bcDF("CLAIM_ID"))
      .agg(max(bcDF("member_qual")),max(bcDF("member_id")),
        sum(bcDF("AMT_BILLED")),sum(bcDF("AMT_PAID")),sum(bcDF("AMT_ALLOWED")),sum(bcDF("AMT_DEDUCT")),sum(bcDF("AMT_COPAY")),sum(bcDF("AMT_COINS")),sum(bcDF("AMT_COB")),
        min(bcDF("ROW_TYPE").as("ROW_TYPE")),sum(bcDF("SVYEARMO").as("SV_YEARMO")),
        count(bcDF("ROW_TYPE")).as("lines")
      )

    println("The expected mem count is " + output.show(10))


  }

}
