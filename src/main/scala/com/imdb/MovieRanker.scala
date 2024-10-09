package com.imdb

import org.apache.spark.sql.SparkSession

object MovieRanker {

  def main(args: Array[String]): Unit = {

    //Loading Configs
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
    val top10Movies = Utils.calculateTopMovies(titleRatings, titleBasics, movieRankerConfigs.numberOfTopMovies)

    println("Top 10 Movies:")
    top10Movies.show(truncate = false)

    // Get tconst list for top 10 movies
    val top10Tconst = top10Movies.select("tconst").as[String].collect().toSeq

    // Read principals and names
    val titlePrincipals = MovieData.readTitlePrincipals(spark, movieRankerConfigs.titlePrincipalsPath, top10Tconst)
    val nameBasics = MovieData.readNameBasics(spark, movieRankerConfigs.nameBasicsPath)

    // Get top persons
    val topPersons = Utils.getTopPersons(titlePrincipals, nameBasics, movieRankerConfigs.numberOfTopPersons)

    println("Most Credited Persons in Top 10 Movies:")
    topPersons.show(truncate = false)

    // List different titles of the 10 movies
    println("Titles of Top 10 Movies:")
    top10Movies.select("primaryTitle").show(truncate = false)

    // Stop Spark Session
    spark.stop()
  }

}
