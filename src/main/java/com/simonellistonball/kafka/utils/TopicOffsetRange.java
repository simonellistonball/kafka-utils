package com.simonellistonball.kafka.utils;

import de.vandermeer.asciitable.AsciiTable;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class TopicOffsetRange {
    public static void main(String[] args) {
        Options options = new Options();
        {
            Option o = new Option("t", "topic", true, "Topic to operate on");
            o.setArgName("TOPIC");
            o.setRequired(true);
            options.addOption(o);
        }
        {
            Option o = new Option("b", "bootstrap-servers", true, "Comma separated list of brokers to bootstrap from");
            o.setArgName("BOOTSTRAP_SERVERS");
            o.setRequired(true);
            options.addOption(o);
        }
        {
            Option o = new Option("f", "properties", true, "Properties file for Kafka connection");
            o.setArgName("PROPERTIES");
            o.setRequired(false);
            options.addOption(o);
        }

        try {
            CommandLineParser parser = new PosixParser();
            CommandLine cmd = null;
            try {
                cmd = parser.parse(options, args);
            } catch (ParseException pe) {
                final HelpFormatter usageFormatter = new HelpFormatter();
                pe.printStackTrace();
                usageFormatter.printHelp("TopicOffsetRange", null, options, null, true);
                System.exit(-1);
            }

            String bootstrap = cmd.getOptionValue("b");
            String topic = cmd.getOptionValue("t");
            String properties = cmd.getOptionValue("f");

            getOffsets(bootstrap, topic, properties);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static class Result {
        private TopicPartition topic;
        private Long start;
        private Long end;

        public Result(TopicPartition topic, Long start, Long end) {
            this.topic = topic;
            this.start = start;
            this.end = end;
        }
    }

    private static void getOffsets(String bootstrap, final String topic, String properties) throws IOException {
        final KafkaConsumer consumer = new KafkaConsumer(getConfigs(bootstrap, properties));
        List<PartitionInfo> parts = consumer.partitionsFor(topic);

        List<TopicPartition> pis = parts.stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .collect(Collectors.toList());

        Map<TopicPartition, Long> begin = consumer.beginningOffsets(pis);
        Map<TopicPartition, Long> end = consumer.endOffsets(pis);

        Map<TopicPartition, Result> map1 = begin.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> new Result(v.getKey(), v.getValue(), null)));
        Map<TopicPartition, Result> map2 = end.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> new Result(v.getKey(), null, v.getValue())));

        Map<TopicPartition, Result> output = Stream.concat(map1.entrySet().stream(), map2.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey,
                        Map.Entry::getValue,
                        (v1, v2) -> new Result(v1.topic,
                                v1.start == null ? v2.start : v1.start,
                                v1.end == null ? v2.end : v1.end
                        )));

        AsciiTable at = new AsciiTable();
        at.addRule();
        at.addRow("Topic", "Start", "End", "Length");
        at.addRule();
        output.entrySet().stream().sorted((a,b) -> comparePartitions(a,b)).forEach(s -> {
            at.addRow(s.getValue().topic, s.getValue().start, s.getValue().end, s.getValue().end - s.getValue().start);

        });
        at.addRule();
        System.out.println(at.render());
    }

    private static int comparePartitions(Map.Entry<TopicPartition, Result> a, Map.Entry<TopicPartition, Result> b) {
        Function<TopicPartition, String> ts = (t) -> String.format("%s-%d", t.topic(), t.partition());
        return ts.apply(a.getKey()).compareTo(ts.apply(b.getKey()));
    }

    private static Properties getConfigs(String bootstrap, String propsFile) throws IOException {
        Properties props = new Properties();
        if (propsFile != null) props.load(new FileInputStream(new File(propsFile)));
        props.setProperty("bootstrap.servers", bootstrap);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        return props;
    }
}
