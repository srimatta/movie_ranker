package com.imdb

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, count, desc}

/**
 * The `Utils` object contains utility functions for processing IMDb datasets.
 * These functions perform operations such as calculating top-ranked movies and identifying
 * the most credited persons associated with those movies.
 */

object Utils {

  /**
   * Calculates the top N movies based on a custom ranking formula.
   *
   * The ranking is determined by multiplying the ratio of a movie's number of votes to
   * the average number of votes across all movies by the movie's average rating.
   *
   * @param titleRatings     A DataFrame containing movie ratings information.
   *                         Expected to have columns: "tconst", "averageRating", "numVotes".
   * @param titleBasics      A DataFrame containing basic movie information.
   *                         Expected to have columns: "tconst", "primaryTitle".
   * @param numberOfTopMovies The number of top-ranked movies to return.
   * @return                 A DataFrame containing the top N movies with columns:
   *                         "tconst", "primaryTitle", "averageRating", "numVotes", "ranking".
   */
  def calculateTopMovies(titleRatings: DataFrame, titleBasics: DataFrame, numberOfTopMovies: Int): DataFrame = {

    // Compute average number of votes
    val avgNumVotes = titleRatings.agg(avg("numVotes").alias("avgVotes")).first().getAs[Double]("avgVotes")

    val rankedTitles = titleRatings.withColumn(
      "ranking",
      (col("numVotes") / avgNumVotes) * col("averageRating")
    )

    rankedTitles.join(titleBasics, Seq("tconst"))
      .select("tconst", "primaryTitle", "averageRating", "numVotes", "ranking")
      .orderBy(desc("ranking"))
      .limit(numberOfTopMovies)
  }

  /**
   * Identifies the top N most credited persons associated with the top movies.
   *
   * This function aggregates the number of movies each person is credited in and returns
   * the top individuals based on their movie counts.
   *
   * @param titlePrincipals  A DataFrame containing principal cast and crew information.
   *                         Expected to have columns: "tconst", "nconst".
   * @param nameBasics       A DataFrame containing basic person information.
   *                         Expected to have columns: "nconst", "primaryName".
   * @param topPersonsCount  The number of top persons to return based on movie credits.
   * @return                 A DataFrame containing the top N persons with columns:
   *                         "primaryName", "movieCount".
   */
  def getTopPersons(titlePrincipals: DataFrame, nameBasics: DataFrame, topPersonsCount: Int): DataFrame = {
    val principalsWithNames = titlePrincipals.join(nameBasics, Seq("nconst"), "left")
      .select("tconst", "primaryName")

    principalsWithNames.groupBy("primaryName")
      .agg(count("tconst").alias("movieCount"))
      .orderBy(desc("movieCount"))
      .limit(topPersonsCount)
  }
}
