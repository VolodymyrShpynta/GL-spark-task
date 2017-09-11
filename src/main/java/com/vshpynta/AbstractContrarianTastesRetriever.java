package com.vshpynta;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

/**
 * The Base abstract class that holds common properties of 'contrarian tastes' retriever implementations.
 * 'Contrarian tastes' - movies that are well-loved by users who dislike movies most users like.
 */
@Slf4j
@RequiredArgsConstructor
public abstract class AbstractContrarianTastesRetriever implements Serializable {
    public static final String LINE_SEPARATOR = "\n";
    public static final String COLON = ":";
    public static final String COLUMNS_SEPARATOR = ",";
    public static final String FILES_MASK = "*.txt";
    public static final String PATH_SEPARATOR = "/";

    @NonNull
    protected final String movieRatingsDirLocation;
    @NonNull
    protected final String movieTitlesFileLocation;
    @NonNull
    protected final String outputDirLocation;
    @NonNull
    protected final Integer minRateUsersNumber;
    @NonNull
    protected final Integer topMoviesNumber;
    @NonNull
    protected final Integer contrarianUsersNumber;
    @NonNull
    protected final Integer partitionsNumber;
    @NonNull
    protected final Boolean localMode;

    /**
     * Retrieves the TOP contrarian user's highest ranked movie
     * and stores the result in the specified folder.
     * The result is in a CSV format file.
     */
    public abstract void retrieveContrarianTastes();
}
