package com.vshpynta.test.utils;

import lombok.experimental.UtilityClass;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static com.vshpynta.test.utils.FileUtils.contentOf;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


/**
 * Shortcut to compare the content of two files.
 */
@UtilityClass
public class Assert {

    public static final String LINE_SEPARATOR = "\n";

    public static void sameContent(final String actualFilePath, final String expectedFilePath) {
        final String actualContent = contentOf(actualFilePath);
        final String expectedContent = contentOf(expectedFilePath);
        assertThat(actualContent, is(expectedContent));
    }

    public static void sameContent(final File actualFile, final File expectedFile) {
        final String actualContent = contentOf(actualFile);
        final String expectedContent = contentOf(expectedFile);
        assertThat(actualContent, is(expectedContent));
    }

    public static void sameLinesInAnyOrder(final File actualFile, final File expectedFile) {
        List<String> actualLines = getLines(actualFile);
        List<String> expectedLines = getLines(expectedFile);
        assertThat(actualLines, is(expectedLines));
    }

    private static List<String> getLines(File file) {
        return Arrays.stream(contentOf(file).split(LINE_SEPARATOR))
                .map(String::trim)
                .sorted()
                .collect(toList());

    }
}
