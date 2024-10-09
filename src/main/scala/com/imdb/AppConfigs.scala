package com.imdb

import com.typesafe.config.ConfigFactory

import java.io.File

/**
 * Case class representing the configuration parameters for the Movie Ranker application.
 *
 * @param master              The Spark master URL (e.g., "local[*]").
 * @param logLevel            The logging level (e.g., "ERROR", "WARN", "INFO", "DEBUG").
 * @param nameBasicsPath      The file path to the `name.basics.tsv` dataset.
 * @param titleBasicsPath     The file path to the `title.basics.tsv` dataset.
 * @param titlePrincipalsPath The file path to the `title.principals.tsv` dataset.
 * @param titleRatingsPath    The file path to the `title.ratings.tsv` dataset.
 * @param numberOfTopMovies   The number of top movies to identify.
 * @param numberOfTopPersons  The number of top persons to list.
 */
case class MovieRankerConfigs(
                               master: String,
                               logLevel: String,
                               nameBasicsPath: String,
                               titleBasicsPath: String,
                               titlePrincipalsPath: String,
                               titleRatingsPath: String,
                               numberOfTopMovies: Int,
                               numberOfTopPersons: Int
                             )
/**
 * AppConfigs Object responsible for loading and parsing the application configurations.
 */

object AppConfigs {
  def loadAppConfigs(configsPath: String = "application.conf"): MovieRankerConfigs = {
    val config = ConfigFactory.parseFile(new File(configsPath)).getConfig("movieranks_configs")

    MovieRankerConfigs(
      master = config.getString("master"),
      logLevel = config.getString("logLevel"),
      nameBasicsPath = config.getString("nameBasicsPath"),
      titleBasicsPath = config.getString("titleBasicsPath"),
      titlePrincipalsPath = config.getString("titlePrincipalsPath"),
      titleRatingsPath = config.getString("titleRatingsPath"),
      numberOfTopMovies = config.getInt("numberOfTopMovies"),
      numberOfTopPersons = config.getInt("numberOfTopPersons")
    )
  }

}
