package com.vshpynta.test.utils;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import lombok.experimental.UtilityClass;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Arrays.stream;

@UtilityClass
public class FileUtils {

    public static String contentOf(final String filePath) {
        try {
            return Files.toString(new File(filePath), Charsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String contentOf(final File file) {
        try {
            return Files.toString(file, Charsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Remove given files
     *
     * @param files given files
     */
    public static void removeFiles(final File... files) {
        if (files == null) {
            return;
        }
        removeFiles(stream(files));
    }

    /**
     * Remove files of given stream
     *
     * @param files given files stream
     */
    public static void removeFiles(final Stream<File> files) {
        if (files == null) {
            return;
        }
        files.filter(Objects::nonNull)
                .filter(File::exists)
                .forEach(File::delete);
    }

    public static void removeFolder(final String folderPath) {
        removeFolder(new File(folderPath));
    }

    public static void removeFolder(final File folder) {
        removeFolders(folder.listFiles(File::isDirectory));
        removeFiles(folder.listFiles(file -> !file.isDirectory()));
        removeFiles(folder);
    }

    public static void removeFolders(final File... folders) {
        if (folders == null) {
            return;
        }
        removeFolders(stream(folders));
    }

    public static void removeFolders(final Stream<File> folders) {
        if (folders == null) {
            return;
        }
        folders.filter(Objects::nonNull)
                .filter(File::exists)
                .forEach(FileUtils::removeFolder);
    }
}
