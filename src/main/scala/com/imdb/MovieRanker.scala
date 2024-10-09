package com.imdb

import org.apache.spark.sql.SparkSession

/**
 * The `MovieRanker` object serves as the entry point for the Movie Ranker application.
 * It orchestrates the entire data processing workflow, including reading IMDb datasets,
 * computing top-ranked movies, identifying the most credited persons, and displaying the results.
 */

object MovieRanker {

  def main(args: Array[String]): Unit = {

    //Loading Application Configs
    val movieRankerConfigs: MovieRankerConfigs = AppConfigs.loadAppConfigs()

    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("MovieRanker")
      .master(movieRankerConfigs.master)
      .getOrCreate()

    spark.sparkContext.setLogLevel(movieRankerConfigs.logLevel)
    import spark.implicits._

    // Read data
    val titleBasics = MovieData.readTitleBasics(spark, movieRankerConfigs.titleBasicsPath)
    val titleRatings = MovieData.readTitleRatings(spark, movieRankerConfigs.titleRatingsPath)

    // Compute top 10 movies
    val topRatedMovies = Utils.calculateTopMovies(titleRatings, titleBasics, movieRankerConfigs.numberOfTopMovies)

    println("Top-rated Movies:")
    topRatedMovies.show(truncate = false)

    // Get tconst list for top-rated movies
    val topRatedMoviesTconst = topRatedMovies.select("tconst").as[String].collect().toSeq

    // Read principals and names
    val titlePrincipals = MovieData.readTitlePrincipals(spark, movieRankerConfigs.titlePrincipalsPath, topRatedMoviesTconst)
    val nameBasics = MovieData.readNameBasics(spark, movieRankerConfigs.nameBasicsPath)

    // Get top persons
    val topPersons = Utils.getTopPersons(titlePrincipals, nameBasics, movieRankerConfigs.numberOfTopPersons)

    println("Most Credited Persons in Top Rated Movies:")
    topPersons.show(truncate = false)

    // List different titles of the 10 movies
    println("Titles of Top Movies:")
    topRatedMovies.select("primaryTitle").show(truncate = false)

    // Stop Spark Session
    spark.stop()
  }

}
