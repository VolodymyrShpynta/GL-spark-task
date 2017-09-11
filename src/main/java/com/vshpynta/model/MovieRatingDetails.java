package com.vshpynta.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Data access object class for output information about highest ranked movie of 'contrarian user'.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MovieRatingDetails implements Comparable<MovieRatingDetails>, Serializable {
    private Integer customerId;
    private String movieTitle;
    private String movieReleaseYear;
    private String ratingDate;

    @Override
    public int compareTo(MovieRatingDetails o) {
        if (movieReleaseYear.compareTo(o.getMovieReleaseYear()) == 0) {
            return o.getMovieTitle().compareTo(movieTitle); //prefer alphabetical order of title
        }
        return movieReleaseYear.compareTo(o.getMovieReleaseYear()); //prefer most recent publication
    }
}
