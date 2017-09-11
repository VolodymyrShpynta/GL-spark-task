package com.vshpynta.model;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

/**
 * Data access object class for input information about movie rating.
 */
@Data
@Builder
public class RatingInfo implements Serializable {
    private Integer movieId;
    private Integer customerId;
    private Integer rating;
    private String ratingDate;
}
