package com.vshpynta.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.action.VersionArgumentAction;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * Performs command line utility operations
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CommandLineUtils {

    public static final String MOVIE_RATINGS_DIR_LOCATION = "movie-ratings-dir";
    public static final String MOVIE_TITLES_FILE_LOCATION = "movie-titles-file";
    public static final String OUTPUT_DIR_LOCATION = "output-dir";
    public static final String MIN_RATE_USERS_NUMBER = "min-rate-users-number";
    public static final String TOP_MOVIES_NUMBER = "top-movies-number";
    public static final String CONTRARIAN_USERS_NUMBER = "contrarian-users-number";
    public static final String PARTITIONS_NUMBER = "partitions-number";

    public static final String ARG_PREFIX = "--";
    public static final String LOCAL_MODE = "local-mode";
    public static final String USE_RDD_IMPL = "use-rdd-impl";

    /**
     * Get {@link Namespace} instance based on provided args
     *
     * @param args - arguments of command line
     * @return Created {@link Namespace} instance based on provided args
     */
    public static Namespace getNamespace(final String[] args) {
        final String usage = "These application can be launched using the 'bin/spark-submit' script";

        final ArgumentParser parser = ArgumentParsers.newArgumentParser(usage)
                .defaultHelp(true)
                .description("Retrieves the contrarian tastes");

        parser.addArgument("-v", "--version")
                .action(new VersionArgumentAction())
                .help("show the aggregator version and exit");

        parser.addArgument(ARG_PREFIX + MOVIE_RATINGS_DIR_LOCATION, "-mrd")
                .dest(MOVIE_RATINGS_DIR_LOCATION)
                .type(String.class)
                .required(true)
                .help("path to directory with movies ratings files.");

        parser.addArgument(ARG_PREFIX + MOVIE_TITLES_FILE_LOCATION, "-mtf")
                .dest(MOVIE_TITLES_FILE_LOCATION)
                .type(String.class)
                .required(true)
                .help("path to file with movies titles.");

        parser.addArgument(ARG_PREFIX + OUTPUT_DIR_LOCATION, "-o")
                .dest(OUTPUT_DIR_LOCATION)
                .type(String.class)
                .required(true)
                .help("path to output directory were the result data will be stored.");

        parser.addArgument(ARG_PREFIX + MIN_RATE_USERS_NUMBER, "-mrun")
                .dest(MIN_RATE_USERS_NUMBER)
                .type(Integer.class)
                .required(false)
                .setDefault(50)
                .help("the minimum number of TOP movies rated users");

        parser.addArgument(ARG_PREFIX + TOP_MOVIES_NUMBER, "-tmn")
                .dest(TOP_MOVIES_NUMBER)
                .type(Integer.class)
                .required(false)
                .setDefault(5)
                .help("the number of TOP movies");

        parser.addArgument(ARG_PREFIX + CONTRARIAN_USERS_NUMBER, "-cun")
                .dest(CONTRARIAN_USERS_NUMBER)
                .type(Integer.class)
                .required(false)
                .setDefault(25)
                .help("the number of contrarian users");

        parser.addArgument(ARG_PREFIX + PARTITIONS_NUMBER, "-pn")
                .dest(PARTITIONS_NUMBER)
                .type(Integer.class)
                .required(false)
                .setDefault(1000)
                .help("the number of Spark partitions");

        parser.addArgument(ARG_PREFIX + LOCAL_MODE, "-lm")
                .dest(LOCAL_MODE)
                .choices(true, false)
                .setDefault(false)
                .type(Boolean.class)
                .required(false)
                .help("boolean flag that turns Application into local mode for testing purposes, in this mode Application accepts local files paths.\n" +
                        "Example: --local-mode true or -lm true");

        parser.addArgument(ARG_PREFIX + USE_RDD_IMPL, "-urdd")
                .dest(USE_RDD_IMPL)
                .choices(true, false)
                .setDefault(false)
                .type(Boolean.class)
                .required(false)
                .help("boolean flag that indicates Application to use RDD implementation of ContrarianTastesRetriever" +
                        "Example: --use-rdd-impl true or -urdd true");

        try {
            return parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        throw new IllegalStateException("Error parsing application args");
    }
}
