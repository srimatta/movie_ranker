package com.imdb

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

/**
 * The `MovieData` object provides methods to read and process various IMDb datasets.
 * Each method is responsible for reading a specific dataset, applying necessary filters,
 * and selecting relevant columns for further analysis.
 */

object MovieData {

  /**
   * Reads the Title Basics dataset and filters it to include only movies.
   *
   * @param spark    The active SparkSession.
   * @param dataPath The file path to the Title Basics dataset.
   * @return A DataFrame containing `tconst` and `primaryTitle` of movies.
   */
  def readTitleBasics(spark: SparkSession, dataPath: String): DataFrame = {

    spark.read
      .option("sep", "\t")
      .schema(MovieDataSchemas.titleBasicsSchema)
      .csv(dataPath)
      .filter(col("titleType") === "movie")
      .select("tconst", "primaryTitle")
  }

  /**
   * Reads the Title Ratings dataset and filters it based on a minimum number of votes.
   *
   * @param spark    The active SparkSession.
   * @param dataPath The file path to the Title Ratings dataset.
   * @param numVotes The minimum number of votes required for a title to be included. Defaults to 500.
   * @return A DataFrame containing ratings information for titles with at least `numVotes` votes.
   */
  def readTitleRatings(spark: SparkSession, dataPath: String, numVotes: Int = 500): DataFrame = {
    spark.read
      .option("sep", "\t")
      .schema(MovieDataSchemas.titleRatingsSchema)
      .csv(dataPath)
      .filter(col("numVotes") >= numVotes)
  }

  /**
   * Reads the Title Principals dataset and filters it to include only the top movie titles.
   *
   * @param spark       The active SparkSession.
   * @param dataPath    The file path to the Title Principals dataset.
   * @param topTconst A sequence of `tconst` identifiers representing the top movies.
   * @return A DataFrame containing principal cast and crew information for the top movies.
   */
  def readTitlePrincipals(spark: SparkSession, dataPath: String, topTconst: Seq[String]): DataFrame = {
    spark.read
      .option("sep", "\t")
      .schema(MovieDataSchemas.titlePrincipalsSchema)
      .csv(dataPath)
      .filter(col("tconst").isin(topTconst: _*))
  }

  /**
   * Reads the Name Basics dataset and selects relevant person information.
   *
   * @param spark    The active SparkSession.
   * @param dataPath The file path to the Name Basics dataset.
   * @return A DataFrame containing `nconst`  and `primaryName` .
   */
  def readNameBasics(spark: SparkSession, dataPath: String): DataFrame = {
    spark.read
      .option("sep", "\t")
      .schema(MovieDataSchemas.nameBasicsSchema)
      .csv(dataPath)
      .select("nconst", "primaryName")
  }
}
