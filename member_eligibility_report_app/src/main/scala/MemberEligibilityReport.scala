package org.eligibility.report


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MemberEligibilityReport {

  private case class MemberEligibility(member_id: String, first_name: String, middle_name: String, last_name: String)
  private case class MemberMonths(eligibility_number: String, member_id: String,
                                  eligibility_effective_date: String, eligibility_member_month: String)
  private case class MemberMonthsPerYear(member_id: String, Year: String, Month: String, TotalMemberMonths: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Member Eligibility Report")
      .config("spark.master", "local[*]")
      .getOrCreate()

    import spark.implicits._

    // Define schema for CSV data
    val memberEligibilitySchema = StructType(Seq(
      StructField("member_id", StringType, nullable = false),
      StructField("first_name", StringType, nullable = false),
      StructField("middle_name", StringType, nullable = false),
      StructField("last_name", StringType, nullable = false)
    ))

    val memberMonthsSchema = StructType(Seq(
      StructField("eligibility_number", StringType, nullable = false),
      StructField("member_id", StringType, nullable = false),
      StructField("eligibility_effective_date", StringType, nullable = false),
      StructField("eligibility_member_month", StringType, nullable = false)
    ))

    // Read CSV data
    val memberEligibilityDF = spark.read.schema(memberEligibilitySchema)
      .csv("/Users/vijay/OneDrive/Desktop/Marketing/member_eligibility_report_app/member_eligibility_report_app/src/main/resources/member_eligibility.csv")
      .as[MemberEligibility]

    val memberMonthsDF = spark.read
      .schema(memberMonthsSchema)
      .csv("/Users/vijay/OneDrive/Desktop/Marketing/member_eligibility_report_app/member_eligibility_report_app/src/main/resources/member_months.csv")
      .as[MemberMonths]

    // Task 1: Calculate total number of member months per member
    val totalMemberMonthsDF = memberMonthsDF
      .join(memberEligibilityDF, Seq("member_id"))
      .groupBy("member_id", "first_name", "middle_name", "last_name")
      .agg(count("eligibility_member_month").alias("TotalMemberMonths"))
      .orderBy("member_id")

    totalMemberMonthsDF.show(10)

    // Write output partitioned by MemberID in "output/task1" path
  totalMemberMonthsDF.write.partitionBy("member_id").json("output/task1")

    // Task 2: Calculate total number of member months per member per year
    val memberMonthsPerYearDF = memberMonthsDF
      .join(memberEligibilityDF, Seq("member_id"))
      .withColumn("Year", year($"eligibility_effective_date"))
      .withColumn("Month", month($"eligibility_effective_date"))
      .groupBy("member_id", "Year", "Month")
      .agg(count("eligibility_member_month").alias("TotalMemberMonths"))
      .orderBy("member_id", "Year", "Month")
      .as[MemberMonthsPerYear]

    memberMonthsPerYearDF.show(10)


    spark.stop()
  }
}
