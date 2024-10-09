package com.imdb

import com.typesafe.config.ConfigFactory

import java.io.File

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
