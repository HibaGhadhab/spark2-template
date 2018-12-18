package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object exe1 {
  //val spark = SessionBuilder.buildSession()
  //import spark.implicits._
  def exec1(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._


    val demographieDF = spark.read
      .json("data/input/demographie_par_commune.json")
    demographieDF.show

    //question 1
    demographieDF
      .select($"Population")
      .agg(sum($"Population"))
      .show

    //question 2
    demographieDF
      .groupBy($"Departement")
      .agg(sum($"Population").as("TotalPopulation"))
      .orderBy($"TotalPopulation".desc)
      .show

    //question 3
    val departementDF = spark.read
      .text("data/input/departements.txt")
      .withColumn("name",split(col("value"),","))
      .select(col("name")(0).as("name"),col("name")(1).as("departement"))
    departementDF.show

    demographieDF
      .groupBy($"Departement")
      .agg(sum($"Population").as("TotalPopulation"))
      .orderBy($"TotalPopulation".desc)
      .join(departementDF, Seq("departement"))
      .show

  }
}
