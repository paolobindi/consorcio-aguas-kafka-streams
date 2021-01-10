package com.soits.serde;

import com.soits.dto.QBalanceRegistryResultDto;
import com.soits.dto.QbalanceDto;
import com.soits.serde.json.MyJsonDeserializer;
import com.soits.serde.json.MyJsonSerializer;
import org.apache.kafka.common.serialization.Serdes;


public class QBalanceRegistryResultSerde extends Serdes.WrapperSerde<QBalanceRegistryResultDto>
{
    public QBalanceRegistryResultSerde()
    {
        super(new MyJsonSerializer<>(), new MyJsonDeserializer<>(QBalanceRegistryResultDto.class));
    }
}
