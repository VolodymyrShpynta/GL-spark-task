package com.vshpynta;

import com.vshpynta.model.MovieInfo;
import com.vshpynta.model.MovieRatingDetails;
import com.vshpynta.model.RatingInfo;
import com.vshpynta.utils.CollectionUtils;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

/**
 * The Spark RDD implementation of {@link AbstractContrarianTastesRetriever}.
 */
@Slf4j
public class ContrarianTastesRetrieverRddImpl extends AbstractContrarianTastesRetriever {

    @Builder
    private ContrarianTastesRetrieverRddImpl(String movieRatingsDirLocation,
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

        try (JavaSparkContext ctx = new JavaSparkContext(createConfig())) {

            JavaPairRDD<Integer, Set<RatingInfo>> movieRatingsRdd = createMovieRatingsRdd(ctx)
                    .partitionBy(new HashPartitioner(partitionsNumber))
                    .cache();

            JavaPairRDD<Integer, MovieInfo> moviesInfoRdd = createMoviesInfoRdd(ctx)
                    .partitionBy(new HashPartitioner(partitionsNumber))
                    .cache();

            Set<Integer> topMovies = retrieveTopMovies(moviesInfoRdd, movieRatingsRdd);
            Broadcast<Set<Integer>> broadcastTopMovies = ctx.broadcast(topMovies);

            Set<Integer> contrarianCustomers = retrieveContrarianCustomers(movieRatingsRdd, broadcastTopMovies);
            Broadcast<Set<Integer>> broadcastContrarianCustomers = ctx.broadcast(contrarianCustomers);

            JavaRDD<MovieRatingDetails> contrarianLovedMovies = getCustomersMuchLovedMovies(ctx, broadcastContrarianCustomers, moviesInfoRdd, movieRatingsRdd);
            saveToCsv(contrarianLovedMovies);
        }
        log.info("Execution time (ms): " + (System.nanoTime() - startTime) / 1_000_000);
    }


    /**
     * Create PairRDD of (movieId, movieRatingsInfos)
     *
     * @param ctx
     * @return
     */
    private JavaPairRDD<Integer, Set<RatingInfo>> createMovieRatingsRdd(JavaSparkContext ctx) {
        return ctx.wholeTextFiles(composeRatingsFilesPath(), partitionsNumber)
                .mapToPair(fileNameContentTuple -> {
                    String fileContent = fileNameContentTuple._2();
                    int firstRowIndex = fileContent.indexOf(LINE_SEPARATOR);
                    String firstRow = fileContent.substring(0, firstRowIndex); //The first row with movie id
                    String movieIdStr = firstRow.substring(0, firstRow.indexOf(COLON));
                    Integer movieId = Integer.valueOf(movieIdStr.trim());
                    String[] ratingRows = fileContent.substring(firstRowIndex + 1).split(LINE_SEPARATOR);
                    return new Tuple2<>(movieId,
                            Arrays.stream(ratingRows)
                                    .map(ratingRow -> ratingRow.split(COLUMNS_SEPARATOR))
                                    .filter(ratingColumns -> ratingColumns.length == 3)
                                    .map(ratingColumns -> createRatingInfo(movieId, ratingColumns))
                                    .collect(toSet()));
                });
    }

    private RatingInfo createRatingInfo(Integer movieId, String[] ratingColumns) {
        return RatingInfo.builder()
                .movieId(movieId)
                .customerId(Integer.valueOf(ratingColumns[0].trim()))
                .rating(Integer.valueOf(ratingColumns[1].trim()))
                .ratingDate(ratingColumns[2].trim())
                .build();
    }

    /**
     * Create PairRDD of (movieId, movieInfo)
     *
     * @param ctx
     * @return
     */
    private JavaPairRDD<Integer, MovieInfo> createMoviesInfoRdd(JavaSparkContext ctx) {
        return ctx.textFile(movieTitlesFileLocation, partitionsNumber)
                .map(row -> row.split(COLUMNS_SEPARATOR))
                .filter(columns -> columns.length == 3)
                .mapToPair(columns -> {
                    Integer movieId = Integer.valueOf(columns[0].trim());
                    return new Tuple2<>(movieId,
                            MovieInfo.builder()
                                    .id(movieId)
                                    .releaseYear(columns[1].trim())
                                    .title(columns[2].trim())
                                    .build());
                });
    }

    /**
     * Retrieve IDs of top movies
     *
     * @param moviesInfoRdd   - PairRDD of (movieId, movieInfo)
     * @param movieRatingsRdd - PairRDD of (movieId, movieRatingsInfos)
     * @return
     */
    private Set<Integer> retrieveTopMovies(JavaPairRDD<Integer, MovieInfo> moviesInfoRdd,
                                           JavaPairRDD<Integer, Set<RatingInfo>> movieRatingsRdd) {
        JavaPairRDD<Integer, Double> movieAverageRatingRdd = calculateAverageRatings(movieRatingsRdd);
        return movieAverageRatingRdd
                // get PairRDD of (movieId, Tuple2(averageMovieRating, MovieInfo))
                .join(moviesInfoRdd)
                .top(topMoviesNumber, new RatingMovieDetailsComparator())
                .stream()
                        // get top movies IDs
                .map(Tuple2::_1)
                .collect(toSet());
    }

    /**
     * Returns an PairRDD of (movieId, averageMovieRating)
     *
     * @param movieRatingsRdd - PairRDD of (movieId, movieRatingsInfos)
     * @return
     */
    private JavaPairRDD<Integer, Double> calculateAverageRatings(JavaPairRDD<Integer, Set<RatingInfo>> movieRatingsRdd) {
        return movieRatingsRdd
                .filter(movieRatings -> movieRatings._2().size() > minRateUsersNumber)
                .mapValues(ratings ->
                        ratings.stream()
                                .map(RatingInfo::getRating)
                                .mapToInt(r -> r)
                                .average()
                                .getAsDouble());
    }

    /**
     * Returns IDs of contrarian customers
     *
     * @param movieRatingsRdd    - PairRDD of (movieId, movieRatingsInfos)
     * @param broadcastTopMovies - top movies IDs
     * @return
     */
    private Set<Integer> retrieveContrarianCustomers(JavaPairRDD<Integer, Set<RatingInfo>> movieRatingsRdd, Broadcast<Set<Integer>> broadcastTopMovies) {
        return movieRatingsRdd
                // keep only top movies ratings
                .filter(movieRatingTuple -> broadcastTopMovies.getValue().contains(movieRatingTuple._1()))
                        // transform PairRDD(movieId, Set<RatingInfo>) to PairRDD(movieId, RatingInfo)
                .flatMapValues(ratingInfos -> ratingInfos)
                        // get RatingInfo from Tuple2(movieId, RatingInfo)
                .map(Tuple2::_2)
                        // map movieRatingsInfos by customerId: Tuple2(customerId, RatingInfo)
                .mapToPair(ratingInfo -> new Tuple2<>(ratingInfo.getCustomerId(), ratingInfo))
                        //group by customerId
                .groupByKey()
                        // filter out customers not rated ALL top movies
                .filter(customerRatingInfos -> isCustomerRatedAllMovies(customerRatingInfos, broadcastTopMovies.getValue()))
                        // get only ratings of top movies
                .mapValues(ratingInfos -> getMatchingMoviesRatings(ratingInfos, broadcastTopMovies.getValue()))
                        // get average of top movies ratings
                .mapValues(CollectionUtils::average)
                .top(contrarianUsersNumber, new ContrarianRatingsComparator())
                .stream()
                        // get Contrarian customer id
                .map(Tuple2::_1)
                .collect(toSet());
    }

    /**
     * Returns true if collectionOfCustomerRates contains all specified movies IDs
     *
     * @param customerRatingInfos - Tuple2 of (customerId, collectionOfCustomerRates)
     * @param movies              - specified movies IDs
     * @return
     */
    private boolean isCustomerRatedAllMovies(Tuple2<Integer, Iterable<RatingInfo>> customerRatingInfos, Set<Integer> movies) {
        // Since the number of movies is in the hundreds of thousands (< 1 000 000),
        // the size of HashSet is < ~24MB ( (16 bytes (object overhead) + 4 bytes (int)+ 4 bytes (padding)) * 1 000 000).
        // Therefore it could be stored in the memory.
        final Set<Integer> ratedMovies = new HashSet<>();
        customerRatingInfos._2()
                .forEach(ratingInfo -> ratedMovies.add(ratingInfo.getMovieId()));
        return ratedMovies.containsAll(movies);
    }

    /**
     * Returns ratings values of specified movies
     *
     * @param ratingInfos   - movies ratings infos, from which only the ratings of movies with specified IDs should be selected
     * @param moviesToMatch - specified movies IDs
     * @return
     */
    private List<Integer> getMatchingMoviesRatings(Iterable<RatingInfo> ratingInfos, Set<Integer> moviesToMatch) {
        List<Integer> matchedMoviesRatings = new LinkedList<>();
        ratingInfos.forEach(ratingInfo -> {
            if (moviesToMatch.contains(ratingInfo.getMovieId())) {
                matchedMoviesRatings.add(ratingInfo.getRating());
            }
        });
        return matchedMoviesRatings;
    }

    private JavaRDD<MovieRatingDetails> getCustomersMuchLovedMovies(JavaSparkContext ctx,
                                                                    Broadcast<Set<Integer>> customers,
                                                                    JavaPairRDD<Integer, MovieInfo> moviesInfoRdd,
                                                                    JavaPairRDD<Integer, Set<RatingInfo>> movieRatingsRdd) {
        // PairRDD (customerId, RatingInfo)
        JavaPairRDD<Integer, RatingInfo> customersRates = mapRatingsBySuppliedCustomers(customers, movieRatingsRdd)
                .cache();
        // Map (customerId, maxRate)
        Broadcast<Map<Integer, Integer>> customersMaxRates = ctx.broadcast(getCustomersMaxRates(customersRates).collectAsMap());

        return customersRates
                //keep only max customers rates
                .filter(customersRateTuple -> isMaxRate(customersMaxRates, customersRateTuple))
                .map(Tuple2::_2)
                        //map ratingInfo by movieId
                .mapToPair(ratingInfo -> new Tuple2<>(ratingInfo.getMovieId(), ratingInfo))
                .join(moviesInfoRdd)
                        //get joined result (from Tuple2(movieId, Tuple2(RatingInfo, MovieInfo)) )
                .map(Tuple2::_2)
                .map(this::createMovieRatingDetails)
                        //map movieRatingDetails by customerId
                .mapToPair(movieRatingDetails -> new Tuple2<>(movieRatingDetails.getCustomerId(), movieRatingDetails))
                        // get most recent and in alphabetical order of title
                .reduceByKey((details1, details2) -> details1.compareTo(details2) > 0 ? details1 : details2)
                .map(Tuple2::_2);
    }

    private JavaPairRDD<Integer, Integer> getCustomersMaxRates(JavaPairRDD<Integer, RatingInfo> customersRdd) {
        return customersRdd
                .mapValues(RatingInfo::getRating)
                        // get max customer's rate
                .reduceByKey((rating1, rating2) -> rating1 > rating2 ? rating1 : rating2);
    }

    /**
     * Returns an PairRDD (customerId, RatingInfo) of specified customers IDs
     *
     * @param customers       - specified customers IDs
     * @param movieRatingsRdd - PairRDD (movieId, Set<RatingInfo>)
     * @return
     */
    private JavaPairRDD<Integer, RatingInfo> mapRatingsBySuppliedCustomers(Broadcast<Set<Integer>> customers,
                                                                           JavaPairRDD<Integer, Set<RatingInfo>> movieRatingsRdd) {
        return movieRatingsRdd
                // transform PairRDD (movieId, Set<RatingInfo>) to RDD (RatingInfo)
                .flatMap(movieRatingTuple -> movieRatingTuple._2().iterator())
                        //keep RatingInfo only of supplied customers
                .filter(ratingInfo -> customers.getValue().contains(ratingInfo.getCustomerId()))
                .mapToPair(ratingInfo -> new Tuple2<>(ratingInfo.getCustomerId(), ratingInfo));
    }

    private boolean isMaxRate(Broadcast<Map<Integer, Integer>> customersMaxRates,
                              Tuple2<Integer, RatingInfo> customersRateTuple) {
        RatingInfo ratingInfo = customersRateTuple._2();
        return Optional.of(customersMaxRates.getValue())
                .map(maxRates -> maxRates.get(ratingInfo.getCustomerId()))
                .filter(maxRate -> maxRate.equals(ratingInfo.getRating()))
                .isPresent();
    }

    private MovieRatingDetails createMovieRatingDetails(Tuple2<RatingInfo, MovieInfo> ratingMovieInfoTuple) {
        return MovieRatingDetails.builder()
                .customerId(ratingMovieInfoTuple._1().getCustomerId())
                .ratingDate(ratingMovieInfoTuple._1().getRatingDate())
                .movieTitle(ratingMovieInfoTuple._2().getTitle())
                .movieReleaseYear(ratingMovieInfoTuple._2().getReleaseYear())
                .build();
    }

    private void saveToCsv(JavaRDD<MovieRatingDetails> contrarianLovedMovies) {
        contrarianLovedMovies
                .map(movieRatingDetails -> format("%s,%s,%s,%s",
                        movieRatingDetails.getCustomerId(),
                        movieRatingDetails.getMovieTitle(),
                        movieRatingDetails.getMovieReleaseYear(),
                        movieRatingDetails.getRatingDate()))
                .coalesce(1) // place all data to a single partition
                .saveAsTextFile(outputDirLocation);
    }

    private SparkConf createConfig() {
        SparkConf sparkConf = new SparkConf().setAppName("ContrarianTastesRetrieverRddImpl");
        if (localMode) {
            sparkConf.setMaster("local");
        }
        return sparkConf;
    }

    //In order to increase performance the files mask is used
    private String composeRatingsFilesPath() {
        String ratingsFilesPath = movieRatingsDirLocation.trim();
        if (ratingsFilesPath.endsWith(PATH_SEPARATOR)) {
            return ratingsFilesPath + FILES_MASK;
        }
        return ratingsFilesPath + PATH_SEPARATOR + FILES_MASK;
    }

    /**
     * Comparator of Tuple2(movieId, Tuple2(averageMovieRating, MovieInfo)).
     * Comparator prefers movie with larger averageMovieRating.
     * If average ratings of two movies (averageMovieRating) are the same, comparator prefers
     * the most recent movie release year, then alphabetical order of movie title.
     */
    private static class RatingMovieDetailsComparator implements Comparator<Tuple2<Integer, Tuple2<Double, MovieInfo>>>, Serializable {
        @Override
        public int compare(Tuple2<Integer, Tuple2<Double, MovieInfo>> idRatingMovieDetails1, Tuple2<Integer, Tuple2<Double, MovieInfo>> idRatingMovieDetails2) {
            Tuple2<Double, MovieInfo> ratingMovieDetails1 = idRatingMovieDetails1._2();
            Tuple2<Double, MovieInfo> ratingMovieDetails2 = idRatingMovieDetails2._2();
            if (ratingMovieDetails1._1().compareTo(ratingMovieDetails2._1()) == 0) {
                MovieInfo movieDetails1 = ratingMovieDetails1._2();
                MovieInfo movieDetails2 = ratingMovieDetails2._2();
                if (movieDetails1.getReleaseYear().compareTo(movieDetails2.getReleaseYear()) == 0) {
                    return movieDetails1.getTitle().compareTo(movieDetails2.getTitle());
                }
                return movieDetails2.getReleaseYear().compareTo(movieDetails1.getReleaseYear());
            }
            return ratingMovieDetails1._1().compareTo(ratingMovieDetails2._1());
        }
    }

    /**
     * Comparator of Tuple2(customerId, averageRatingOfTopMovies).
     * Comparator prefers customer who has given the lowest average rating of the top movies.
     * If average rates of two customers are the same, comparator prefers customer with the lower ID.
     */
    private static class ContrarianRatingsComparator implements Comparator<Tuple2<Integer, Double>>, Serializable {
        @Override
        public int compare(Tuple2<Integer, Double> customerRatingTuple1, Tuple2<Integer, Double> customerRatingTuple2) {
            if (customerRatingTuple1._2().compareTo(customerRatingTuple2._2()) == 0) {
                return customerRatingTuple2._1().compareTo(customerRatingTuple1._1());
            }
            return customerRatingTuple2._2().compareTo(customerRatingTuple1._2());
        }
    }
}
