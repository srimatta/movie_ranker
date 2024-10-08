package com.example

import com.imdb.{MovieData, Utils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object MovieRanker {

  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("MovieRanker")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // Define the path to IMDb data
    // You can pass this as an argument or set it here
    val dataPath = if (args.length > 0) args(0) else "/Users/srinivas/Desktop/project_apps/movie_ranker/imdb_data/"

    // Read data
    val titleBasics = MovieData.readTitleBasics(spark, dataPath)
    val titleRatings = MovieData.readTitleRatings(spark, dataPath)

    // Compute top 10 movies
    val top10Movies = Utils.calculateTopMovies(titleRatings, titleBasics, 10)

    println("Top 10 Movies:")
    top10Movies.show(truncate = false)

    // Get tconst list for top 10 movies
    val top10Tconst = top10Movies.select("tconst").as[String].collect().toSeq

    // Read principals and names
    val titlePrincipals = MovieData.readTitlePrincipals(spark, dataPath, top10Tconst)
    val nameBasics = MovieData.readNameBasics(spark, dataPath)

    // Get top persons
    val topPersons = Utils.getTopPersons(titlePrincipals, nameBasics, 10)

    println("Most Credited Persons in Top 10 Movies:")
    topPersons.show(truncate = false)

    // List different titles of the 10 movies
    println("Titles of Top 10 Movies:")
    top10Movies.select("primaryTitle").show(truncate = false)

    // Stop Spark Session
    spark.stop()
  }

}
