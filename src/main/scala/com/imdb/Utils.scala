package com.imdb

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, count, desc}

object Utils {

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

  def getTopPersons(titlePrincipals: DataFrame, nameBasics: DataFrame, topPersonsCount: Int): DataFrame = {
    val principalsWithNames = titlePrincipals.join(nameBasics, Seq("nconst"), "left")
      .select("tconst", "primaryName")

    principalsWithNames.groupBy("primaryName")
      .agg(count("tconst").alias("movieCount"))
      .orderBy(desc("movieCount"))
      .limit(topPersonsCount)
  }
}
