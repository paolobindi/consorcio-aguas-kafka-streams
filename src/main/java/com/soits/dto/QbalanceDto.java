package com.soits.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.Builder;

@ToString
@EqualsAndHashCode
@Getter
@JsonDeserialize(builder = QbalanceDto.Builder.class)
@Builder(builderClassName = "Builder", toBuilder = true)
@JsonIgnoreProperties(ignoreUnknown = true)

public class QbalanceDto
{
    @JsonProperty("code")
    private final String code;

    @JsonProperty("count")
    private final Integer count;

    @JsonProperty("measuring")
    private final Integer measuring;

    @JsonProperty("date_measuring")
    private final String date_measuring;

    @JsonPOJOBuilder(withPrefix = "")
    @JsonIgnoreProperties(ignoreUnknown = true)

    public static class Builder {
    }


}
