package com.soits.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.soits.dto.QBalanceRegistryResultDto;
import com.soits.dto.QbalanceDto;
import com.soits.dto.RegistryDto;
import com.soits.serde.MySerdesFactory;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;

public class StationMeasuringTopology
{
    public void createTopology(StreamsBuilder builder)
    {
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        // KStream input topic:
        //        station-measuring-split-no-avro
        KStream<String, JsonNode> stationMeasuringTransactions = builder.stream("station-measuring-split-no-avro",
                Consumed.with(Serdes.String(), jsonSerde));

        // Get only streams with Q Type Measuring
        KStream<String, JsonNode> stationMeasuringQ = stationMeasuringTransactions
                .filter((key,value) -> value.get("type_measuring").asText().equals("Q"));

        // Send data to topic
        //         station-measuring-q
        stationMeasuringQ.to("station-measuring-q", Produced.with(Serdes.String(), jsonSerde));

        // Aggregate and sum
        // 1. Create the initial json object for balances
        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("code", "");
        initialBalance.put("count", 0);
        initialBalance.put("measuring", 0);
        initialBalance.put("date_measuring", Instant.ofEpochMilli(0L).toString());

        // 2. Aggregate and sum data for Q type Measuring for every code station
        KTable<String, JsonNode> qbalanceCount = stationMeasuringQ
                .groupByKey(Serialized.with(Serdes.String(), jsonSerde))
                .aggregate(
                        () -> initialBalance,
                        (key, transaction, balance) -> newBalance(key, transaction, balance),
                        Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("station-measuring-agg")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(jsonSerde)
                );

        // 3. Send data to intermediate topic:
        //         qbalance
        qbalanceCount.toStream().to("qbalance", Produced.with(Serdes.String(), jsonSerde));

        // Join topics
        //         qbalance
        //         sourcelast-stations_registry
        final KTable<String, QbalanceDto> qbalanceTable =
                builder.stream("qbalance",
                        Consumed.with(Serdes.String(), MySerdesFactory.qbalanceSerde()))
                        .map((key, value) -> new KeyValue<>(value.getCode(), value))
                        .toTable(Materialized.<String, QbalanceDto, KeyValueStore<Bytes, byte[]>>
                                as("qbalance-materialized")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(MySerdesFactory.qbalanceSerde()));

        final KTable<String, RegistryDto> registryTable =
                builder.stream("sourcelast-stations_registry",
                        Consumed.with(Serdes.String(), MySerdesFactory.registrySerde()))
                        .map((key, value) -> new KeyValue<>(value.getCode(), value))
                        .toTable(Materialized.<String, RegistryDto, KeyValueStore<Bytes, byte[]>>
                                as("cledos-registy-materialized")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(MySerdesFactory.registrySerde()));

        final KTable<String, QBalanceRegistryResultDto> balanceRegistryResultTable = qbalanceTable.join(registryTable,
                QbalanceDto::getCode,
                (balance, registry) -> {
                    QBalanceRegistryResultDto qBalanceRegistryResultDto = new QBalanceRegistryResultDto();
                    qBalanceRegistryResultDto.setCode(registry.getCode());
                    qBalanceRegistryResultDto.setCount(balance.getCount());
                    qBalanceRegistryResultDto.setMeasuring(balance.getMeasuring());
                    qBalanceRegistryResultDto.setDate_measuring(balance.getDate_measuring());
                    qBalanceRegistryResultDto.setLatitude(registry.getLatitude());
                    qBalanceRegistryResultDto.setLongitude(registry.getLongitude());
                    return qBalanceRegistryResultDto;
                },
                Materialized.<String, QBalanceRegistryResultDto, KeyValueStore<Bytes, byte[]>>
                        as("qbalace-registry-materialized")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(MySerdesFactory.qBalanceRegistryResultSerde())
        );

        // 4. Send data to final result joined topic
        //         qbalace-registry-result
        balanceRegistryResultTable.toStream()
                .map((key, value) -> new KeyValue<>(value.getCode(), value))
                .peek((key,value) -> System.out.println("(empResultTable) key,vale = " + key  + "," + value))
                .to("qbalace-registry-result", Produced.with(Serdes.String(), MySerdesFactory.qBalanceRegistryResultSerde()));
    }

    /**
     *  Generate Balance from streams.
     *
     * @param key
     * @param transaction
     * @param balance
     * @return JsonNode
     */
    private static JsonNode newBalance(String key, JsonNode transaction, JsonNode balance)
    {
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("code", key);
        newBalance.put("count", balance.get("count").asInt() + 1);
        newBalance.put("measuring", balance.get("measuring").asInt() + transaction.get("measuring").asInt());

        // Calculate Time
        Long balanceEpoch = Instant.parse(balance.get("date_measuring").asText()).toEpochMilli();
        Long transactionEpoch = Instant.parse(transaction.get("date_measuring").asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));

        newBalance.put("date_measuring", newBalanceInstant.toString());

        return newBalance;
    }
}
