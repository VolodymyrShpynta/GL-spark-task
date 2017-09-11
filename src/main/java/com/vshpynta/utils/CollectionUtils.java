package com.vshpynta.utils;

import lombok.experimental.UtilityClass;

import java.util.Collection;

@UtilityClass
public class CollectionUtils {

    /**
     * Calculates average value of collection elements.
     * @param collection - the specified collection.
     * @return the average value of collection elements.
     */
    public static Double average(Collection<Integer> collection) {
        return collection.stream()
                .mapToInt(v -> v)
                .average()
                .getAsDouble();
    }
}
