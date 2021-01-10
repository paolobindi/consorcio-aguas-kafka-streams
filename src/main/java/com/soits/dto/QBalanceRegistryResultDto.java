package com.soits.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class QBalanceRegistryResultDto
{
    @JsonProperty("code")
    private String code;

    @JsonProperty("latitude")
    private String latitude;

    @JsonProperty("longitude")
    private String longitude;

    @JsonProperty("count")
    private Integer count;

    @JsonProperty("measuring")
    private Integer measuring;

    @JsonProperty("date_measuring")
    private String date_measuring;

}
