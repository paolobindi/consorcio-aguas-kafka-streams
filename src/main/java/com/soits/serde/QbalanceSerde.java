package com.soits.serde;

import com.soits.dto.QbalanceDto;
import com.soits.serde.json.MyJsonDeserializer;
import com.soits.serde.json.MyJsonSerializer;
import org.apache.kafka.common.serialization.Serdes;

public class QbalanceSerde extends Serdes.WrapperSerde<QbalanceDto>
{
    public QbalanceSerde()
    {
        super(new MyJsonSerializer<>(), new MyJsonDeserializer<>(QbalanceDto.class));
    }
}
