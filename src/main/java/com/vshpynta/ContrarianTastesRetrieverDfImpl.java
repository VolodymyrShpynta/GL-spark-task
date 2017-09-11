package com.vshpynta;

import com.vshpynta.model.MovieInfo;
import com.vshpynta.model.MovieRatingDetails;
import com.vshpynta.model.RatingInfo;
import com.vshpynta.utils.CollectionUtils;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

import static com.vshpynta.utils.SchemaUtils.createSchema;
import static java.util.Arrays.asList;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;

/**
 * The Spark Dataframe implementation of {@link AbstractContrarianTastesRetriever}.
 */
@Slf4j
public class ContrarianTastesRetrieverDfImpl extends AbstractContrarianTastesRetriever {

    public static final String ID = "id";
    public static final String MOVIE_ID = "movieId";
    public static final String AVG_RATING = "avgRating";
    public static final String RELEASE_YEAR = "releaseYear";
    public static final String TITLE = "title";
    public static final String RATING = "rating";
    public static final String MAX_RATING = "max_rating";
    public static final String CUSTOMER_ID = "customerId";
    public static final String MOVIE_TITLE = "movieTitle";
    public static final String MOVIE_RELEASE_YEAR = "movieReleaseYear";
    public static final String RATING_DATE = "ratingDate";

    @Builder
    private ContrarianTastesRetrieverDfImpl(String movieRatingsDirLocation,
                                            String movieTitlesFileLocation,
                                            String outputDirLocation,
                                            Integer minRateUsersNumber,
                                            Integer topMoviesNumber,
                                            Integer contrarianUsersNumber,
                                            Integer partitionsNumber,
                                            Boolean localMode) {
        super(movieRatingsDirLocation,
                movieTitlesFileLocation,
                outputDirLocation,
                minRateUsersNumber,
                topMoviesNumber,
                contrarianUsersNumber,
                partitionsNumber,
                localMode);
    }

    @Override
    public void retrieveContrarianTastes() {
        long startTime = System.nanoTime();
        SparkConf sparkConf = createConfig();

        try (JavaSparkContext ctx = new JavaSparkContext(sparkConf);
             SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()) {

            Dataset<MovieInfo> movieInfoDS = createMoviesInfoDS(sparkSession)
                    .repartition(partitionsNumber, col(ID))
                    .cache();
            Dataset<RatingInfo> ratingsInfoDS = createRatingInfoDS(ctx, sparkSession)
                    .repartition(partitionsNumber, col(MOVIE_ID))
                    .cache();
            Dataset<Row> topMoviesDS = retrieveTopMovies(movieInfoDS, ratingsInfoDS)
                    .cache();
            Dataset<Row> contrarianCustomersDS = retrieveContrarianCustomers(ctx, sparkSession, topMoviesDS, ratingsInfoDS);
            Dataset<MovieRatingDetails> contrarianLovedMovies = getCustomersMuchLovedMovies(contrarianCustomersDS, movieInfoDS, ratingsInfoDS);
            saveToCsv(contrarianLovedMovies);
        }
        log.info("Execution time (ms): " + (System.nanoTime() - startTime) / 1_000_000);
    }

    private Dataset<MovieInfo> createMoviesInfoDS(SparkSession sparkSession) {
        return sparkSession.read()
                .option("header", "false")
                .csv(movieTitlesFileLocation)
                .filter(row -> row.size() == 3)
                .map(row -> MovieInfo.builder()
                                .id(Integer.valueOf(row.getString(0).trim()))
                                .releaseYear(row.getString(1).trim())
                                .title(row.getString(2).trim())
                                .build(),
                        Encoders.bean(MovieInfo.class));
    }

    private Dataset<RatingInfo> createRatingInfoDS(JavaSparkContext ctx, SparkSession sparkSession) {
        JavaRDD<RatingInfo> movieRatingsRdd = ctx.wholeTextFiles(composeRatingsFilesPath(), partitionsNumber)
                .flatMap(fileNameContentTuple -> parseRatings(fileNameContentTuple._2()).iterator());
        return sparkSession.createDataFrame(movieRatingsRdd, RatingInfo.class)
                .as(Encoders.bean(RatingInfo.class));
    }

    //In order to increase performance the files mask is used
    private String composeRatingsFilesPath() {
        String ratingsFilesPath = movieRatingsDirLocation.trim();
        if (ratingsFilesPath.endsWith(PATH_SEPARATOR)) {
            return ratingsFilesPath + FILES_MASK;
        }
        return ratingsFilesPath + PATH_SEPARATOR + FILES_MASK;
    }

    private Set<RatingInfo> parseRatings(String fileContent) {
        int firstRowIndex = fileContent.indexOf(LINE_SEPARATOR);
        String firstRow = fileContent.substring(0, firstRowIndex); //The first row with movie id
        String movieIdStr = firstRow.substring(0, firstRow.indexOf(COLON));
        Integer movieId = Integer.valueOf(movieIdStr.trim());
        String[] ratingRows = fileContent.substring(firstRowIndex + 1).split(LINE_SEPARATOR);
        return Arrays.stream(ratingRows)
                .map(ratingRow -> ratingRow.split(COLUMNS_SEPARATOR))
                .filter(ratingColumns -> ratingColumns.length == 3)
                .map(ratingColumns -> createMovieRating(movieId, ratingColumns))
                .collect(Collectors.toSet());
    }

    private Dataset<Row> retrieveTopMovies(Dataset<MovieInfo> movieInfoDS, Dataset<RatingInfo> ratingsInfoDS) {
        Dataset<Row> avgRatingsDS = calculateAverageRatings(ratingsInfoDS);
        return avgRatingsDS.join(movieInfoDS, avgRatingsDS.col(MOVIE_ID).equalTo(movieInfoDS.col(ID)))
                .orderBy(avgRatingsDS.col(AVG_RATING).desc(),
                        movieInfoDS.col(RELEASE_YEAR).desc(),
                        movieInfoDS.col(TITLE).asc())
                .select(MOVIE_ID)
                .limit(topMoviesNumber);
    }

    private Dataset<Row> calculateAverageRatings(Dataset<RatingInfo> ratingsInfoDS) {
        return ratingsInfoDS
                .groupBy(MOVIE_ID)
                .agg(avg(RATING).alias(AVG_RATING),
                        count(MOVIE_ID).alias("ratingsCount"))
                .select(MOVIE_ID, AVG_RATING)
                .where("ratingsCount > " + minRateUsersNumber);
    }

    private Dataset<Row> retrieveContrarianCustomers(JavaSparkContext ctx,
                                                     SparkSession sparkSession,
                                                     Dataset<Row> topMoviesDS,
                                                     Dataset<RatingInfo> ratingInfoDS) {
        Broadcast<Set<Integer>> broadcastTopMovies = ctx.broadcast(toSet(topMoviesDS.as(Encoders.INT())));
        JavaRDD<Row> customerAvgRatesOfTopMovies = ratingInfoDS
                // keep only top movies ratings
                .join(broadcast(topMoviesDS), MOVIE_ID)
                .select(CUSTOMER_ID, MOVIE_ID, RATING)
                .toJavaRDD()
                        // Tuple2(customerId, Tuple2(movieId, rating))
                .mapToPair(row -> new Tuple2<>(row.getInt(0), new Tuple2<>(row.getInt(1), row.getInt(2))))
                        // group by customerId
                .groupByKey()
                        // filter out customers not rated ALL top movies
                .filter(customerMoviesRatings -> isCustomerRatedAllMovies(customerMoviesRatings, broadcastTopMovies.getValue()))
                        // get only top movies ratings
                .mapValues(movieRatingPairs -> getMatchingMoviesRatings(movieRatingPairs, broadcastTopMovies.getValue()))
                        // get average of top movies ratings
                .mapValues(CollectionUtils::average)
                        // map to row(customerId,avgRating)
                .map(customerAvgRatingTuple -> RowFactory.create(customerAvgRatingTuple._1(), customerAvgRatingTuple._2()));
        return sparkSession.createDataFrame(customerAvgRatesOfTopMovies,
                createSchema(asList(CUSTOMER_ID, AVG_RATING), asList(IntegerType, DoubleType)))
                .orderBy(col(AVG_RATING).asc(), col(CUSTOMER_ID).asc())
                .select(col(CUSTOMER_ID))
                .limit(contrarianUsersNumber);
    }

    private Boolean isCustomerRatedAllMovies(Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>> customerMoviesRatings, Set<Integer> movies) {
        // Since the number of movies is in the hundreds of thousands (< 1 000 000),
        // the size of HashSet is < ~24MB ( (16 bytes (object overhead) + 4 bytes (int)+ 4 bytes (padding)) * 1 000 000).
        // Therefore it could be stored in the memory.
        final Set<Integer> ratedMovies = new HashSet<>();
        customerMoviesRatings._2().forEach(movieRatingTuple -> ratedMovies.add(movieRatingTuple._1()));
        return ratedMovies.containsAll(movies);
    }

    private List<Integer> getMatchingMoviesRatings(Iterable<Tuple2<Integer, Integer>> movieRatingsTuples, Set<Integer> moviesToMatch) {
        List<Integer> matchedMoviesRatings = new LinkedList<>();
        movieRatingsTuples.forEach(movieRatingPair -> {
            if (moviesToMatch.contains(movieRatingPair._1())) {
                matchedMoviesRatings.add(movieRatingPair._2());
            }
        });
        return matchedMoviesRatings;
    }

    private Dataset<MovieRatingDetails> getCustomersMuchLovedMovies(Dataset<Row> customersDS, Dataset<MovieInfo> moviesInfoDS, Dataset<RatingInfo> ratingsInfoDS) {
        Dataset<Row> customersMaxRatesDS = getCustomersMaxRates(customersDS, ratingsInfoDS);
        return ratingsInfoDS
                .join(customersMaxRatesDS, CUSTOMER_ID)
                .where(ratingsInfoDS.col(RATING).equalTo(customersMaxRatesDS.col(MAX_RATING)))
                .join(moviesInfoDS, ratingsInfoDS.col(MOVIE_ID).equalTo(moviesInfoDS.col(ID)))
                .select(ratingsInfoDS.col(CUSTOMER_ID),
                        moviesInfoDS.col(TITLE).alias(MOVIE_TITLE),
                        moviesInfoDS.col(RELEASE_YEAR).alias(MOVIE_RELEASE_YEAR),
                        ratingsInfoDS.col(RATING_DATE))
                .as(Encoders.bean(MovieRatingDetails.class))
                .groupByKey(MovieRatingDetails::getCustomerId, Encoders.INT())
                .reduceGroups((details1, details2) -> details1.compareTo(details2) > 0 ? details1 : details2)
                .map(Tuple2::_2, Encoders.bean(MovieRatingDetails.class));
    }

    private Dataset<Row> getCustomersMaxRates(Dataset<Row> customersDS, Dataset<RatingInfo> ratingsInfoDS) {
        return ratingsInfoDS
                .join(broadcast(customersDS), CUSTOMER_ID)
                .groupBy(CUSTOMER_ID)
                .agg(max(RATING).alias(MAX_RATING));
    }

    private RatingInfo createMovieRating(Integer movieId, String[] ratingColumns) {
        return RatingInfo.builder()
                .movieId(movieId)
                .customerId(Integer.valueOf(ratingColumns[0].trim()))
                .rating(Integer.valueOf(ratingColumns[1].trim()))
                .ratingDate(ratingColumns[2].trim())
                .build();
    }

    private SparkConf createConfig() {
        SparkConf sparkConf = new SparkConf().setAppName("ContrarianTastesRetrieverDfImpl");
        if (localMode) {
            sparkConf.setMaster("local");
        }
        return sparkConf;
    }

    private void saveToCsv(Dataset<MovieRatingDetails> contrarianLovedMovies) {
        contrarianLovedMovies
                .select(CUSTOMER_ID, MOVIE_TITLE, MOVIE_RELEASE_YEAR, RATING_DATE)
                .coalesce(1) // place all data to a single partition
                .write()
                .option("delimiter", COLUMNS_SEPARATOR)
                .csv(outputDirLocation);
    }

    private static <T> Set<T> toSet(Dataset<T> dataset) {
        return new HashSet<>(dataset.collectAsList());
    }
}
