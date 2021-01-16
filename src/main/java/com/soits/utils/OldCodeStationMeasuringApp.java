package com.soits.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.soits.dto.QBalanceRegistryResultDto;
import com.soits.dto.QbalanceDto;
import com.soits.dto.RegistryDto;
import com.soits.serde.MySerdesFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;
import java.util.Properties;

public class OldCodeStationMeasuringApp
{
    public static void main(String[] args)
    {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "station-measuring-paolo");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, JsonNode> stationMeasuringTransactions = builder.stream("station-measuring-split-no-avro",
                Consumed.with(Serdes.String(), jsonSerde));

        // Get only string with Q TypeMeasuring
        KStream<String, JsonNode> stationMeasuringQ = stationMeasuringTransactions
                .filter((key,value) -> value.get("type_measuring").asText().equals("Q"));

        // Enviamos a topic para ver datos
        stationMeasuringQ.to("station-measuring-q",
                Produced.with(Serdes.String(), jsonSerde));


        // create the initial json object for balances
        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("code", "");
        initialBalance.put("count", 0);
        initialBalance.put("measuring", 0);
        initialBalance.put("date_measuring", Instant.ofEpochMilli(0L).toString());

        KTable<String, JsonNode> qbalanceCount = stationMeasuringQ
                .groupByKey(Serialized.with(Serdes.String(), jsonSerde))
                .aggregate(
                        () -> initialBalance,
                        (key, transaction, balance) -> newBalance(key, transaction, balance),
                        Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("station-measuring-agg")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(jsonSerde)
                );

        // Enviamos al topic intermedio los agregados
        qbalanceCount.toStream().to("qbalance", Produced.with(Serdes.String(), jsonSerde));

        // JOIN -> QBalance and Registry
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

        balanceRegistryResultTable.toStream()
                .map((key, value) -> new KeyValue<>(value.getCode(), value))
                .peek((key,value) -> System.out.println("(empResultTable) key,vale = " + key  + "," + value))
                .to("qbalace-registry-result", Produced.with(Serdes.String(), MySerdesFactory.qBalanceRegistryResultSerde()));


        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

        // print the topology
        //streams.localThreadsMetadata().forEach(data -> System.out.println(data));

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static JsonNode newBalance(String key, JsonNode transaction, JsonNode balance)
    {
        // create a new balance json object
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();

        newBalance.put("code", key);
        newBalance.put("count", balance.get("count").asInt() + 1);
        newBalance.put("measuring", balance.get("measuring").asInt() + transaction.get("measuring").asInt());

        Long balanceEpoch = Instant.parse(balance.get("date_measuring").asText()).toEpochMilli();
        Long transactionEpoch = Instant.parse(transaction.get("date_measuring").asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
        newBalance.put("date_measuring", newBalanceInstant.toString());

        return newBalance;
    }
}
