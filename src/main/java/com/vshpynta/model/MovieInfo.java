package com.vshpynta.model;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

/**
 * Data access object class for input information about movie.
 */
@Data
@Builder
public class MovieInfo implements Serializable{
    private Integer id;
    private String releaseYear;
    private String title;
}
