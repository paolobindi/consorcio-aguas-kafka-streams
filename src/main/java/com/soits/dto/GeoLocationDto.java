package com.soits.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class GeoLocationDto
{
    @JsonProperty("lat")
    private String lat;

    @JsonProperty("lon")
    private String lon;
}
