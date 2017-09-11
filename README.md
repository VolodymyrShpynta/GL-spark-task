# GL-spark-task

It's a spark implementation of Netflix Prize Dataset analysis task.

There are two implementation - RDD and Dataframe.
The default one is a Dataframe implementation.
To switch to the RDD implementation specify the next application command line argument: `--use-rdd-impl true`.

NOTE: the RDD implementation shows the better performance on the small amount of data.
But on the big amount of data the Dataframe implementations shows the much better performance results.   

To launch this application, build it and use as part of `bin/spark-submit` script.
Example:
````
./bin/spark-submit \
  --class com.vshpynta.Launcher \
  --master <master-url> \
  /path/to/GL-spark-task-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --movie-ratings-dir <ratings-dir-location> --movie-titles-file <titles-file-location> --output-dir <output-dir-location>
````
It is a full list of application's command line arguments:

- `--movie-ratings-dir` - the path to a directory with movies ratings files.
- `--movie-titles-file` - the path to a file with movies titles.
- `--output-dir` - the path to an output directory were the result data will be stored.
- `--min-rate-users-number` - the minimum number of TOP movies rated users.
- `--top-movies-number` - the number of TOP movies.
- `--contrarian-users-number` - the number of contrarian users.
- `--partitions-number` - the number of Spark partitions.
- `--local-mode` - the boolean flag that turns Application into local mode for testing purposes, in this mode Application accepts local files paths.
- `--use-rdd-impl` - the boolean flag that indicates Application to use RDD implementation of ContrarianTastesRetriever.

======================================
#### ANALYSIS TASK DESCRIPTION
 Using Hadoop MapReduce, Apache Spark, or another distributed computing technology, 
 analyze the Netflix Prize Dataset - http://academictorrents.com/details/9b13183dc4d60676b773c9e2cd6de5e5542cee9a. 
 (Click the "Download" link in the upper right corner of that page, not the "uci.edu" URL near the bottom of the page.) 
 The README in that file describes the format of the data.
 
 We're looking for movies that are well-loved by users who dislike movies most users like.
 
 Find the M movies which have been rated the highest across all users (of movies which have been rated by at least R users). (If there's a tie for the Mth spot, prefer most recent publication then alphabetical order of title.) 
 These are the "top movies."
 
 Of users who have rated all top M movies, find the U users which have given the lowest average rating of the M movies. (If there's a tie for the Uth spot, prefer users with the lower ID.) 
 These are the "contrarian users."
 
 For the U contrarian users, find each user's highest ranked movie. 
 (If there's a tie for each user's top spot, prefer most recent publication then alphabetical order of title.)
 
 Prepare a CSV report with the following columns:
 
 - 	User ID of contrarian user
 - 	Title of highest rated movie by contrarian user
 - 	Year of release of that movie
 - 	Date of rating of that movie

 Note: M, U, and R should be configurable. The recommended default values for the parameters are M = 5, U = 25, and R = 50.
 Note: The dataset that you see is a subset of the production data. The number of movies in in the hundreds of thousands and the number of users is in tens of millions. Design accordingly.
 

======================================
#### TRAINING DATASET DIRECTORY DESCRIPTION

The directory "training_set" contains 17770 files, one
per movie.  The first line of each file contains the movie id followed by a
colon.  Each subsequent line in the file corresponds to a rating from a customer
and its date in the following format:

CustomerID,Rating,Date

- MovieIDs range from 1 to 17770 sequentially.
- CustomerIDs range from 1 to 2649429, with gaps. There are 480189 users.
- Ratings are on a five star (integral) scale from 1 to 5.
- Dates have the format YYYY-MM-DD.

======================================
#### MOVIES FILE DESCRIPTION

Movie information in "movie_titles.txt" is in the following format:

MovieID,YearOfRelease,Title

- MovieID do not correspond to actual Netflix movie ids or IMDB movie ids.
- YearOfRelease can range from 1890 to 2005 and may correspond to the release of
  corresponding DVD, not necessarily its theaterical release.
- Title is the Netflix movie title and may not correspond to
  titles used on other sites.  Titles are in English.