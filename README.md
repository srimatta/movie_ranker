# movie_ranker

Running in command line:

run 'sbt assembly' command which generates target/scala-2.12/movie_ranker-assembly-1.0.jar

Run the below command to see the results.

java -cp /Users/srinivas/Desktop/project_apps/movie_ranker/target/scala-2.12/movie_ranker-assembly-1.0.jar com.imdb.MovieRanker

Running via Docker:

docker build -t movie-ranker-app .

docker run --rm -v "$(pwd)/application_docker.conf:/app/application.conf"  movie-ranker-app

