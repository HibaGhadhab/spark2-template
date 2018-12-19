package sql_practice

import org.apache.spark.sql.functions.{avg, explode, max, min}
import spark_helpers.SessionBuilder

object exe3 {
  def exec1(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    //question 1: how many unique level of difficulties (in one table)
//    toursDF
//      .select($"tourDifficulty")
//      .distinct()
//      .show()

    //question 2: whats is the min/max/avg of tour prices (in one table)
//    toursDF
//      .select($"tourPrice")
//      .agg(min($"tourPrice").as("minimum"),max($"tourPrice").as("maximum"),avg($"tourPrice").as("average"))
//      .show

    //question 3: what is the min/max/avg of price for each level of difficulty (in one table)
//    toursDF
//      .groupBy($"tourDifficulty")
//      .agg(min($"tourPrice").as("minimum"),max($"tourPrice").as("maximum"),avg($"tourPrice").as("average"))
//      .show

    //question 4: what is the min/max/avg of price and min/max/avg of duration(length) for each level of diffuclty
    // (in one table)
//    toursDF
//      .groupBy($"tourDifficulty")
//      .agg(min($"tourPrice").as("minimumPrice"),
//        max($"tourPrice").as("maximumPrice"),
//        avg($"tourPrice").as("averagePrice")
//        ,min($"tourLength").as("minimumLength"),
//        max($"tourLength").as("maximumLength"),
//        avg($"tourLength").as("averageLength"))
//      .show

    //question 5: display the top 10 "tourTags"
    toursDF
      .select(explode($"tourTags"))
      .groupBy($"col".as("tag"))
      .count()
      .orderBy($"count".desc)
      .show

    //question 6: Relationship between top 10 "tourTags" and "tourDiffuclty"
    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col".as("tag"), $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    //question 7: what is the min/max/avg of price in "tourTags" and "tourdifficulty" relationship
    //(sort by average of price)
    toursDF
      .select(explode($"tourTags"), $"tourDifficulty",$"tourPrice")
      .groupBy($"col".as("tag"), $"tourDifficulty")
      .agg(min($"tourPrice").as("minimumPrice"),
          max($"tourPrice").as("maximumPrice"),
         avg($"tourPrice").as("averagePrice"))
      .orderBy($"averagePrice".desc)
      .show

  }
}
