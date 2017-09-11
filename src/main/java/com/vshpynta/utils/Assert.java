package com.vshpynta.utils;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Assert {

    /**
     * Throws an {@link IllegalArgumentException} with specified message if the expression is false.
     * @param expression - expression to verify.
     * @param message - message of exception.
     */
    public static void isTrue(boolean expression, String message) {
        if (!expression) {
            throw new IllegalArgumentException(message);
        }
    }
}
