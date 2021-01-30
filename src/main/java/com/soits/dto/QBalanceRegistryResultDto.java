package com.soits.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class QBalanceRegistryResultDto
{
    @JsonProperty("code")
    private String code;

    @JsonProperty("count")
    private Integer count;

    @JsonProperty("measuring")
    private Integer measuring;

    @JsonProperty("date_measuring")
    private String date_measuring;

    @JsonProperty("location")
    private GeoLocationDto geoLocation;

    // Comment this part to remember that these two fields are null and we can remove them ONLY IF we add
    // JsonInclude at the beginning of the class
    /*
    @JsonProperty("lat")
    private String lat;

    @JsonProperty("lon")
    private String lon;
    */

}
