/**
 * @author Prakash Yenugandula
 * Date  : 2020-11-09
Take a comma delimited topic with value as: userid,color and null key
Filter out bad data: keep only 3 RGB colors. Get the running count of favourite colors overall this to a topic
Get the running count of the favourite colors overall and output this to a topic
Note: A user's FavouriteColor can change over time
https://github.com/simplesteph/kafka-streams-course/blob/master/favourite-colour-java/src/main/java/com/github/simplesteph/udemy/kafka/streams/FavouriteColourApp.java

Commands used to create required topics
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic favourite-colour-input --replication-factor 1 --partitions 1
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic favourite-colour-output --replication-factor 1 --partitions 1 --config cleanup.policy=compact --config delete.retention.ms=100 --config segment.ms=100 --config min.cleanable.dirty.ratio=0.01
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic user-keys-and-colours --replication-factor 1 --partitions 1 --config cleanup.policy=compact --config delete.retention.ms=100 --config segment.ms=100 --config min.cleanable.dirty.ratio=0.01
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic favourite-colour-input

bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group favourite-color
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group favourite-colour --reset-offsets --topic favourite-colour-input --to-earliest --execute
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --topic user-keys-and-colours --property print.key=true --property print.value=true
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --topic favourite-colour-output --property print.key=true --property print.value=true --formatter kafka.tools.DefaultMessageFormatter \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
*/

package kafka.streams.examples;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

public class FavouriteColorApp
{
	public static void main(String[] args) {
		String inputTopic = "favourite-colour-input";
		String outputTopic = "favourite-colour-output";
		String logCompactedTopic = "user-keys-and-colours";
		String appId = "favourite-colour";
		List<String> colors = getAcceptedColors();

		Properties config = getStreamProperties(appId);
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> source = builder.stream(inputTopic);

		KStream<String, String> usersAndColors = source
				.filter((key, value) -> value.contains(","))
				.selectKey((nullKey, userColor) -> userColor.split(",")[0].toLowerCase())
				.mapValues((key, userColor) -> userColor.split(",")[1].toLowerCase())
				.filter((user, color) -> colors.contains(color));
		usersAndColors.to(logCompactedTopic);

		KTable<String, String> usersAndColoursTable = builder.table(logCompactedTopic);
		KTable<String, Long> favouriteColours = usersAndColoursTable
				.groupBy((user, color) -> new KeyValue<>(color, color))
				.count(Materialized.as("colors-count"));

		favouriteColours.toStream().to(outputTopic); //, Produced.with(Serdes.String(), Serdes.Long()));

		Topology topology = builder.build();
		System.out.println("Topology: " + topology.describe());
		KafkaStreams streams = new KafkaStreams(topology, config);
		streams.cleanUp(); // only in dev, not in prod
		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	private static Properties getStreamProperties(String appId) {
		Properties config = new Properties();
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,  Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

		// we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
		config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0"); // mainly used for window buffering across all threads
		return config;
	}

	private static List<String> getAcceptedColors() {
		// return RGB
		return Arrays.asList("red", "green", "blue");
	}
}
