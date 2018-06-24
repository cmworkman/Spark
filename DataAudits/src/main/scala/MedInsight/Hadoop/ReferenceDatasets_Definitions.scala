package MedInsight.Hadoop

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataTypes


/**
  * Created by christopher.workman on 6/23/2018.
  */

class Dates_Table {
  val DATES  = StructField("DATES", DataTypes.DateType)
  val YEARS  = StructField("YEARS", DataTypes.LongType)
  val MONTHS = StructField("MONTHS", DataTypes.StringType)
  val MONTH_NAME  = StructField("MONTH_NAME", DataTypes.StringType)
  val YEAR_MO  = StructField("YEAR_MO", DataTypes.LongType)
  val FIRST_DATE_IN_MONTH  = StructField("FIRST_DATE_IN_MONTH", DataTypes.StringType)
  val LAST_DATE_IN_MONTH  = StructField("LAST_DATE_IN_MONTH", DataTypes.StringType)
  val DAY_OF_MONTH  = StructField("DAY_OF_MONTH", DataTypes.ShortType)
  val WEEKEND_FLAG  = StructField("WEEKEND_FLAG", DataTypes.ShortType)
  val DAY_OF_WEEK  = StructField("DAY_OF_WEEK", DataTypes.ShortType)
  val DAY_OF_YEAR  = StructField("DAY_OF_YEAR", DataTypes.LongType)
  val JULIAN_DATE  = StructField("JULIAN_DATE", DataTypes.LongType)
  val DAY_NAME  = StructField("DAY_NAME", DataTypes.StringType)
  val CALENDAR_QUARTER = StructField("CALENDAR_QUARTER", DataTypes.ShortType)
  val FISCAL_QUARTER  = StructField("FISCAL_QUARTER", DataTypes.StringType)
  val SEASON  = StructField("SEASON", DataTypes.StringType)
  val HOLIDAY_FLAG  = StructField("HOLIDAY_FLAG", DataTypes.StringType)
  val FISCAL_YEAR  = StructField("FISCAL_YEAR", DataTypes.StringType)


  val fields = Array(DATES,YEARS,MONTHS,MONTH_NAME,YEAR_MO,FIRST_DATE_IN_MONTH,LAST_DATE_IN_MONTH,DAY_OF_MONTH,WEEKEND_FLAG,DAY_OF_WEEK,DAY_OF_YEAR,JULIAN_DATE,DAY_NAME,CALENDAR_QUARTER,FISCAL_QUARTER,SEASON,HOLIDAY_FLAG,FISCAL_YEAR)
  val schema = StructType(fields)

}

class RFT_DIS_STAT_Table {
  val DIS_STAT = StructField("DIS_STAT", DataTypes.StringType)
  val DIS_STAT_DESC = StructField("DIS_STAT_DESC", DataTypes.StringType)
  val VER = StructField("VER", DataTypes.StringType)

  val fields = Array(DIS_STAT,DIS_STAT_DESC,VER)
  val schema = StructType(fields)
}

class RFT_ICD9_DIAG_Table {
  val ICD9_DIAG = StructField("ICD9_DIAG", DataTypes.StringType)
  val ICD9_DIAG_DESC = StructField("ICD9_DIAG_DESC", DataTypes.StringType)
  val ICD9_DIAG_MR_LINE = StructField("ICD9_DIAG_MR_LINE", DataTypes.StringType)
  val VER = StructField("VER", DataTypes.StringType)

  val fields = Array(ICD9_DIAG,ICD9_DIAG_DESC,ICD9_DIAG_MR_LINE,VER)
  val schema = StructType(fields)
}

class RFT_ICD9_PROC_Table {
  val ICD9_PROC = StructField("ICD9_PROC", DataTypes.StringType)
  val ICD9_PROC_DESC = StructField("ICD9_PROC_DESC", DataTypes.StringType)
  val ICD9_PROC_MR_LINE = StructField("ICD9_PROC_MR_LINE", DataTypes.StringType)
  val VER = StructField("VER", DataTypes.StringType)

  val fields = Array(ICD9_PROC,ICD9_PROC_DESC,ICD9_PROC_MR_LINE,VER)
  val schema = StructType(fields)
}

class RFT_ICD10_DIAG_Table {
  val ICD10_DIAG = StructField("ICD10_DIAG", DataTypes.StringType)
  val ICD10_DIAG_DESC = StructField("ICD10_DIAG_DESC", DataTypes.StringType)
  val ICD10_DIAG_MR_LINE = StructField("ICD10_DIAG_MR_LINE", DataTypes.StringType)
  val VER = StructField("VER", DataTypes.StringType)

  val fields = Array(ICD10_DIAG,ICD10_DIAG_DESC,ICD10_DIAG_MR_LINE,VER)
  val schema = StructType(fields)
}

class RFT_ICD10_PROC_Table {
  val ICD10_PROC = StructField("ICD10_PROC", DataTypes.StringType)
  val ICD10_PROC_DESC1 = StructField("ICD10_PROC_DESC1", DataTypes.StringType)
  val MR_LINE = StructField("MR_LINE", DataTypes.StringType)
  val MR_LINE_DESC = StructField("MR_LINE_DESC", DataTypes.StringType)
  val YEAR_ADDED = StructField("YEAR_ADDED", DataTypes.StringType)
  val YEAR_DROPPED = StructField("YEAR_DROPPED", DataTypes.StringType)
  val VER = StructField("VER", DataTypes.StringType)

  val fields = Array(ICD10_PROC,ICD10_PROC_DESC1,MR_LINE,MR_LINE_DESC,YEAR_ADDED,YEAR_DROPPED,VER)
  val schema = StructType(fields)
}

class RFT_PAYER_LOB_Table {
  val PAYER_LOB_KEY = StructField("PAYER_LOB_KEY", DataTypes.ShortType)
  val PAYER_LOB = StructField("PAYER_LOB", DataTypes.StringType)

  val fields = Array(PAYER_LOB_KEY,PAYER_LOB)
  val schema = StructType(fields)
}

class RFT_PAYER_TYPE_Table {
  val PAYER_LOB_KEY = StructField("PAYER_LOB_KEY", DataTypes.ShortType)
  val PAYER_TYPE_KEY = StructField("PAYER_TYPE_KEY", DataTypes.ShortType)
  val PAYER_TYPE = StructField("PAYER_TYPE", DataTypes.StringType)
  val PAYER_TYPE_DESC = StructField("PAYER_TYPE_DESC", DataTypes.StringType)

  val fields = Array(PAYER_LOB_KEY,PAYER_TYPE_KEY,PAYER_TYPE,PAYER_TYPE_DESC)
  val schema = StructType(fields)
}

class RFT_POS_Table {
  val POS = StructField("POS", DataTypes.StringType)
  val POS_DESC = StructField("POS_DESC", DataTypes.StringType)
  val POS_SURG = StructField("POS_SURG", DataTypes.StringType)
  val F_NF = StructField("F_NF", DataTypes.StringType)
  val POS_TYPE = StructField("POS_TYPE", DataTypes.StringType)
  val PBP_LINE = StructField("PBP_LINE", DataTypes.StringType)
  val VER = StructField("VER", DataTypes.StringType)

  val fields = Array(POS,POS_DESC,POS_SURG,F_NF,POS_TYPE,PBP_LINE,VER)
  val schema = StructType(fields)
}

class RFT_PROC_CODE_Table {
  val PROC_CODE = StructField("PROC_CODE", DataTypes.StringType)
  val PROC_DESC = StructField("PROC_DESC", DataTypes.StringType)
  val MR_LINE = StructField("MR_LINE", DataTypes.StringType)
  val MR_LINE_PROC2HOP = StructField("MR_LINE_PROC2HOP", DataTypes.StringType)
  val PPACA_MR_LINE = StructField("PPACA_MR_LINE", DataTypes.StringType)
  val PPACA_MR_LINE_PROC2HOP = StructField("PPACA_MR_LINE_PROC2HOP", DataTypes.StringType)
  val PPACA_MEASURE = StructField("PPACA_MEASURE", DataTypes.StringType)
  val PPACA_AGE = StructField("PPACA_AGE", DataTypes.StringType)
  val PPACA_GENDER = StructField("PPACA_GENDER", DataTypes.StringType)
  val PPACA_DIAGNOSIS = StructField("PPACA_DIAGNOSIS", DataTypes.StringType)
  val PPACA_HCPCS = StructField("PPACA_HCPCS", DataTypes.StringType)
  val PROC_FAMILY = StructField("PROC_FAMILY", DataTypes.StringType)
  val PHY_EXAM_REL = StructField("PHY_EXAM_REL", DataTypes.StringType)
  val MHSA_PROC = StructField("MHSA_PROC", DataTypes.StringType)
  val PBP_LINE = StructField("PBP_LINE", DataTypes.StringType)
  val PBP_PREV = StructField("PBP_PREV", DataTypes.StringType)
  val PBP_PROC2HOP = StructField("PBP_PROC2HOP", DataTypes.StringType)
  val VER = StructField("VER", DataTypes.StringType)
  val LONG_DESCRIPTION = StructField("LONG_DESCRIPTION", DataTypes.StringType)

  val fields = Array(PROC_CODE,PROC_DESC,MR_LINE,MR_LINE_PROC2HOP,PPACA_MR_LINE,PPACA_MR_LINE_PROC2HOP,PPACA_MEASURE,PPACA_AGE,PPACA_GENDER,PPACA_DIAGNOSIS,PPACA_HCPCS,PROC_FAMILY,PHY_EXAM_REL,MHSA_PROC,PBP_LINE,PBP_PREV,PBP_PROC2HOP,VER,LONG_DESCRIPTION)
  val schema = StructType(fields)
}

class RFT_REV_CODE_Table {
  val REV_CODE = StructField("REV_CODE", DataTypes.StringType)
  val REV_DESC = StructField("REV_DESC", DataTypes.StringType)
  val MR_LINE_DET = StructField("MR_LINE_DET", DataTypes.StringType)
  val MR_LINE = StructField("MR_LINE", DataTypes.StringType)
  val PBP_LINE = StructField("PBP_LINE", DataTypes.StringType)
  val PBP_LOGIC = StructField("PBP_LOGIC", DataTypes.StringType)
  val ILOCOLD = StructField("ILOCOLD", DataTypes.StringType)
  val ILOC = StructField("ILOC", DataTypes.StringType)
  val VER = StructField("VER", DataTypes.StringType)

  val fields = Array(REV_CODE,REV_DESC,MR_LINE_DET,MR_LINE,PBP_LINE,PBP_LOGIC,ILOCOLD,ILOC,VER)
  val schema = StructType(fields)
}

class RFT_DRG_Table {
  val DRG_TYPE = StructField("DRG_TYPE", DataTypes.StringType)
  val DRG_CODE = StructField("DRG_CODE", DataTypes.StringType)
  val DRG_DESC = StructField("DRG_DESC", DataTypes.StringType)
  val MDC_CODE = StructField("MDC_CODE", DataTypes.StringType)

  val fields = Array(DRG_TYPE,DRG_CODE,DRG_DESC,MDC_CODE)
  val schema = StructType(fields)
}





