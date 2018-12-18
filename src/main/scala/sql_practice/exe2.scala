package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder


object exe2 {
  def exec1(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val sample07DF = spark.read
      .option("delimiter","\t")
      .csv("data/input/sample_07")
      .select($"_c0".as("code"),
      $"_c1".as("description"),
      $"_c2".as("nbremploye7"),
      $"_c3".as("salary7"))
    sample07DF.show

    val sample08DF = spark.read
      .option("delimiter","\t")
      .csv("data/input/sample_08")
      .select($"_c0".as("code"),
      $"_c1".as("description"),
      $"_c2".as("nbremploye8"),
      $"_c3".as("salary8"))
    sample08DF.show

  }
}
