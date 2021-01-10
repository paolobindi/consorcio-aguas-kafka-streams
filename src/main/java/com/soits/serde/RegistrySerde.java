package com.soits.serde;

import com.soits.dto.RegistryDto;
import com.soits.serde.json.MyJsonDeserializer;
import com.soits.serde.json.MyJsonSerializer;
import org.apache.kafka.common.serialization.Serdes;

public class RegistrySerde extends Serdes.WrapperSerde<RegistryDto>
{
    public RegistrySerde()
    {
        super(new MyJsonSerializer<>(), new MyJsonDeserializer<>(RegistryDto.class));
    }
}
