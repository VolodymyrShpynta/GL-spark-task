package com.vshpynta.test;

import com.vshpynta.ContrarianTastesRetrieverDfImpl;
import com.vshpynta.test.utils.Assert;
import com.vshpynta.test.utils.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class ContrarianTastesRetrieverDfImplTest {

    private static final String MOVIE_RATINGS_DIR_LOCATION = "src/test/resources/test-data/training_set";
    private static final String MOVIE_TITLES_FILE_LOCATION = "src/test/resources/test-data/movie_titles.txt";
    private static final String OUTPUT_DIR_LOCATION = "target/contrarian-loved-movies";
    private static final int MIN_RATE_USERS_NUMBER = 5;
    private static final int TOP_MOVIES_NUMBER = 3;
    private static final int CONTRARIAN_USERS_NUMBER = 4;
    private static final int PARTITIONS_NUMBER = 8;
    private static final boolean LOCAL_MODE = true;

    private static final File expectedContrarianLovedMoviesFile = new File("src/test/resources/expected-contrarian-loved-movies.csv");

    @Before
    public void setUp() throws Exception {
        FileUtils.removeFolder(OUTPUT_DIR_LOCATION);
    }

    @Test
    public void retrieveContrarianTastesTest() throws IOException {
        ContrarianTastesRetrieverDfImpl.builder()
                .movieRatingsDirLocation(MOVIE_RATINGS_DIR_LOCATION)
                .movieTitlesFileLocation(MOVIE_TITLES_FILE_LOCATION)
                .outputDirLocation(OUTPUT_DIR_LOCATION)
                .topMoviesNumber(TOP_MOVIES_NUMBER)
                .contrarianUsersNumber(CONTRARIAN_USERS_NUMBER)
                .minRateUsersNumber(MIN_RATE_USERS_NUMBER)
                .partitionsNumber(PARTITIONS_NUMBER)
                .localMode(LOCAL_MODE)
                .build()
                .retrieveContrarianTastes();
        Assert.sameLinesInAnyOrder(getContrarianLovedMoviesFile(), expectedContrarianLovedMoviesFile);
    }

    private File getContrarianLovedMoviesFile() {
        File filesDir = new File(OUTPUT_DIR_LOCATION);
        return Arrays.stream(filesDir.listFiles())
                .filter(file -> file.getName().startsWith("part-"))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Can't find contrarian loved movies file"));
    }
}
