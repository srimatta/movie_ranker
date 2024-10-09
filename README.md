# movie_ranker

### Running in command line:

> run 'sbt assembly' command which generates target/scala-2.12/movie_ranker-assembly-1.0.jar

Keep application.conf same directory where it's running. below are the default application.conf properties

    movieranks_configs {
    master = "local[*]"
    logLevel = "ERROR"
    nameBasicsPath = "imdb_data/name.basics.tsv"
    titleBasicsPath = "imdb_data/title.basics.tsv"
    titlePrincipalsPath = "imdb_data/title.principals.tsv"
    titleRatingsPath = "imdb_data/title.ratings.tsv"
    numberOfTopMovies = 10
    numberOfTopPersons = 10
    }


### Running via Docker:

Building docker Image

> docker build -t movie-ranker-app .

> docker run --rm -v "$(pwd)/application_docker.conf:/app/application.conf"  movie-ranker-app

