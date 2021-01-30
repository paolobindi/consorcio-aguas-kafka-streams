package com.soits.main;

import com.soits.topology.StationMeasuringTopology;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class MainClass
{
    public static void main(String[] args)
    {
        //Logger logger = LoggerFactory.getLogger("ru-rocker-main-class");
        final StreamsBuilder builder = new StreamsBuilder();

        // Kafka Stream Properties
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "station-measuring-soits");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        //props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        // disable cache. DEV only
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        // create station measuring topology
        StationMeasuringTopology stationMeasuringTopology = new StationMeasuringTopology();
        stationMeasuringTopology.createTopology(builder);

        // build topology
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // clean up existing stream (DEV only)
        streams.cleanUp();

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("soits-kafka-stream") {
            @Override
            public void run() {
                //logger.info("Shutting down stream...");
                System.out.println("Shutting down stream...");
                streams.close();
                //logger.info("Stream is stopped");
                System.out.println("Stream is stopped");
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Exception e) {
            //logger.error("Exception: ", e);
            System.out.println("Exception: " + e);
            System.exit(1);
        }
        System.exit(0);
    }
}
