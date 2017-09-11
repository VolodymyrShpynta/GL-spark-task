package com.vshpynta;

import com.vshpynta.utils.CommandLineUtils;
import net.sourceforge.argparse4j.inf.Namespace;

import static com.vshpynta.utils.CommandLineUtils.*;

/**
 * It is an entry point of the application.
 */
public class Launcher {
    public static void main(String[] args) {
        final Namespace ns = CommandLineUtils.getNamespace(args);
        getContrarianTastesRetriever(ns).retrieveContrarianTastes();
    }

    private static AbstractContrarianTastesRetriever getContrarianTastesRetriever(Namespace ns) {
        if (ns.getBoolean(USE_RDD_IMPL)) {
            return createContrarianTastesRetrieverRddImpl(ns);
        }
        return createContrarianTastesRetrieverDfImpl(ns);
    }

    private static ContrarianTastesRetrieverDfImpl createContrarianTastesRetrieverDfImpl(Namespace ns) {
        return ContrarianTastesRetrieverDfImpl.builder()
                .movieRatingsDirLocation(ns.getString(MOVIE_RATINGS_DIR_LOCATION))
                .movieTitlesFileLocation(ns.getString(MOVIE_TITLES_FILE_LOCATION))
                .outputDirLocation(ns.getString(OUTPUT_DIR_LOCATION))
                .topMoviesNumber(ns.getInt(TOP_MOVIES_NUMBER))
                .contrarianUsersNumber(ns.getInt(CONTRARIAN_USERS_NUMBER))
                .minRateUsersNumber(ns.getInt(MIN_RATE_USERS_NUMBER))
                .partitionsNumber(ns.getInt(PARTITIONS_NUMBER))
                .localMode(ns.getBoolean(LOCAL_MODE))
                .build();
    }

    private static ContrarianTastesRetrieverRddImpl createContrarianTastesRetrieverRddImpl(Namespace ns) {
        return ContrarianTastesRetrieverRddImpl.builder()
                .movieRatingsDirLocation(ns.getString(MOVIE_RATINGS_DIR_LOCATION))
                .movieTitlesFileLocation(ns.getString(MOVIE_TITLES_FILE_LOCATION))
                .outputDirLocation(ns.getString(OUTPUT_DIR_LOCATION))
                .topMoviesNumber(ns.getInt(TOP_MOVIES_NUMBER))
                .contrarianUsersNumber(ns.getInt(CONTRARIAN_USERS_NUMBER))
                .minRateUsersNumber(ns.getInt(MIN_RATE_USERS_NUMBER))
                .partitionsNumber(ns.getInt(PARTITIONS_NUMBER))
                .localMode(ns.getBoolean(LOCAL_MODE))
                .build();
    }
}
