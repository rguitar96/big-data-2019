import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._

object App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    /////////////////////////////////////////
    // Exercise 1

    val inputDf = spark
      .read
      .format("csv")
      .option("header", "true")
      .load("data/1997.csv")

    val df = inputDf
      .drop("ArrTime")
      .drop("ActualElapsedTime")
      .drop("AirTime")
      .drop("TaxiIn")
      .drop("Diverted")
      .drop("CarrierDelay")
      .drop("WeatherDelay")
      .drop("NASDelay")
      .drop("SecurityDelay")
      .drop("LateAircraftDelay")

    df
      .schema
      .fields
      .foreach(x => println(x))

    /*
        inputDf.printSchema()
        inputDf.show(15, truncate = false)

        /////////////////////////////////////////
        // Exercise 2

        println("Total number of elements: "+inputDf.count)

        println("Complete list of project names")
        val projectList = inputDf
          .select("project_name")
          .distinct
          .rdd
          .map((r: Row) => r.getString(0))
          .collect
        println(projectList)

        inputDf
          .filter(col("project_name") === "en")
          .groupBy("project_name")
          //.sum("content_size")
          .agg(sum("content_size").as("total_size"))
          .show(truncate = false)

        inputDf
          .filter(col("project_name") === "en")
          .orderBy(col("num_requests").desc)
          .show(5, truncate = false)

        // Now with SQL
        inputDf.createOrReplaceTempView("myFakeTable")
        spark.sql("select count(*) from myFakeTable").show
        spark.sql("select distinct project_name from myFakeTable").show
        spark.sql("select sum(content_size) from myFakeTable where project_name = 'en'").show
        spark.sql("select * from myFakeTable where project_name = 'en' order by num_requests desc limit 5").show

        /////////////////////////////////////////
        // Exercise 3

        println("Project summary")

        val projectSummary = inputDf
          .groupBy("project_name")
          .agg(
            count("page_title").as("num_pages"),
            sum("content_size").as("content_size"),
            mean("num_requests").as("mean_requests"))

        projectSummary.show(projectList.length, truncate = false)

        println("Most visited")
        inputDf
          .join(projectSummary.select("project_name", "mean_requests"), "project_name")
          .filter(col("num_requests") > col("mean_requests"))
          .show

     */
  }
}
