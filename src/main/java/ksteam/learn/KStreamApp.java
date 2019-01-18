package ksteam.learn;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.io.UnsupportedEncodingException;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KStreamApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream("streams-plaintext-input")
                .peek((key, value) -> {
                    try {
                        System.out.println(MessageFormat.format("key:{0},value:{1}", key, new String((byte[]) value, "utf-8")));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                })
                .groupBy((key, value) -> {
                    try {
                        String line = new String((byte[]) value, "utf-8");
                        return line.split(",")[0].getBytes();
                    } catch (Exception ex) {
                        return null;
                    }
                })
                .count()
                .toStream()
                .peek((key, value) -> {
                    System.out.println(MessageFormat.format("key:{0},value:{1}", key, value));
                });


        final Topology topology = builder.build();

        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
