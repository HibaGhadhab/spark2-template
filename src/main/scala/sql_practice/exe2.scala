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
      .filter ($"code" =!= "00-0000")
    sample07DF.show

    val sample08DF = spark.read
      .option("delimiter","\t")
      .csv("data/input/sample_08")
      .select($"_c0".as("code"),
      $"_c1".as("description8"),
      $"_c2".as("nbremploye8"),
      $"_c3".as("salary8"))
      .filter ($"code" =!= "00-0000")
    sample08DF.show

    //question 1: salaries over 100 000
    sample07DF
      .orderBy($"salary7".desc)
      .filter($"salary7" > 100000 && $"code" =!= "00-0000")
      .show

    sample08DF
      .orderBy($"salary8".desc)
      .filter($"salary8" > 100000 && $"code" =!= "00-0000")
      .show

    //question 2: growth
    sample07DF
      .join(sample08DF, Seq("code"))
      .select($"description",$"salary7", $"salary8", ((($"salary8"-$"salary7")/$"salary7")*100).as("growth"))
      .filter($"salary8">$"salary7" && $"code" =!= "00-0000")
      .orderBy($"growth".desc)
      .show

    //question 3: domains of lost jobs
    sample07DF
      .join(sample08DF, Seq("code"))
      .select($"description",$"nbremploye7", $"nbremploye8", ($"nbremploye8"-$"nbremploye7").as("lostJobs"))
      .filter($"nbremploye8">$"nbremploye7" && $"code" =!= "00-0000")
      .orderBy($"lostJobs".desc)
      .show

  }
}
