package com.simonellistonball.kafka.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class ConsumerGroupOffestTweaker {
	public static void main(String[] args) {
		Options options = new Options();
		{
			Option o = new Option("p", "partition", true, "Partition");
			o.setArgName("PARTITION");
			o.setRequired(true);
			options.addOption(o);
		}
		{
			Option o = new Option("g", "group", true, "Consumer group to operate on");
			o.setArgName("GROUP");
			o.setRequired(true);
			options.addOption(o);
		}
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
			Option o = new Option("o", "offest", true, "Commit this offset");
			o.setArgName("OPTION");
			o.setRequired(true);
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
				usageFormatter.printHelp("ConsumerGroupOffestTweaker", null, options, null, true);
				System.exit(-1);
			}

			String bootstrap = cmd.getOptionValue("b");
			String topic = cmd.getOptionValue("t");
			String group = cmd.getOptionValue("g");
			Integer partition = Integer.valueOf(cmd.getOptionValue("p"));
			Long offset = Long.valueOf(cmd.getOptionValue("o"));

			commitOffset(bootstrap, topic, group, partition, offset);

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}

	}

	@SuppressWarnings("unchecked")
	private static void commitOffset(String bootstrap, String topic, String group, int partition, Long offset) {
		@SuppressWarnings("rawtypes")
		KafkaConsumer consumer = new KafkaConsumer(getConfigs(bootstrap, group));

		System.out.println(String.format("Topic: %s, Group %s", topic, group));

		try {
			List<PartitionInfo> parts = consumer.partitionsFor(topic);
			for (PartitionInfo part : parts) {
				System.out.println(String.format("Topic: %s Partition: %d Leader %s", part.topic(), part.partition(),
						part.leader().host()));
			}

			// build the offset to change
			OffsetAndMetadata offsetMeta = new OffsetAndMetadata(offset);
			TopicPartition topicPartition = new TopicPartition(topic, partition);
			Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
			offsets.put(topicPartition, offsetMeta);

			System.out.println(
					String.format("Setting offsets for %s on %s partition %d to %d", group, topic, partition, offset));
			consumer.commitSync(offsets);
			System.out.println("Offset committed");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}

	}

	private static Properties getConfigs(String bootstrap, String group) {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", bootstrap);
		props.put("group.id", group);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "100");
		props.put("security.protocol", "SASL_PLAINTEXT");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		return props;
	}
}
