package spark.udaf

import org.apache.spark.sql.SparkSession

trait FunctionRegistration {
  def registerFunctions(spark: SparkSession): Unit
}