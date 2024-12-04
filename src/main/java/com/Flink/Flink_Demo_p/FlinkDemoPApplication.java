package com.Flink.Flink_Demo_p;

import com.Flink.Flink_Demo_p.Dto.UserEntity;
import com.Flink.Flink_Demo_p.Service.AgeCaluclationFunction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.Properties;

@SpringBootApplication
public class FlinkDemoPApplication {

	public static void main(String[] args) throws Exception {

		try {
			SpringApplication.run(FlinkDemoPApplication.class, args);
			String inputTopic = "inputTopic";
			String EvenTopic = "consumerEven";
			String OddTopic = "consumerOdd";
			//String outputTopic = "EvenTopic";
			String server = "localhost:9092";
			streamCosnumer(inputTopic, EvenTopic, OddTopic, server);
			// System.out.println("Hello v");

		} catch (ProgramInvocationException e) {
			System.err.println("Error invoking program: " + e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			System.err.println("Unexpected error occurred: " + e.getMessage());
			e.printStackTrace();
		}
	}
		public static void streamCosnumer(String inputTopic,String EvenTopic, String OddTopic, String server) throws Exception {
			final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			FlinkKafkaConsumer011<UserEntity> flinkkafkaconsumer = createStringConsumerForTopic(inputTopic,server);
			FlinkKafkaProducer011<UserEntity> evenProducer = createKafkaProducer(EvenTopic, server);
			FlinkKafkaProducer011<UserEntity> oddProducer =createKafkaProducer(OddTopic, server);
			DataStream<UserEntity> inputstream= env.addSource(flinkkafkaconsumer);
			inputstream.print("input Stream");

			DataStream<UserEntity> processedStream = inputstream
					.map(new AgeCaluclationFunction());
			processedStream.print("processed stream");
			DataStream<UserEntity> evenstream = processedStream
					.filter(user -> user.getAge() % 2 == 0);
			evenstream.print("even stream");
			DataStream<UserEntity> odddstream = processedStream
					.filter(user -> user.getAge() % 2 != 0);
			odddstream.print("odd stream");

			evenstream.addSink(evenProducer);
			odddstream.addSink(oddProducer);
			try {
				env.execute("Flink Kafka Streaming Job");
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	public static FlinkKafkaConsumer011<UserEntity> createStringConsumerForTopic(String topic, String kafkaAddress) {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", kafkaAddress);
		props.setProperty("group.id", "flink-consumer-group");  // Explicit group.id
		return new FlinkKafkaConsumer011<>(topic, new DeserializationSchema<UserEntity>() {
			private final ObjectMapper objectMapper = new ObjectMapper();

			@Override
			public UserEntity deserialize(byte[] message) throws IOException {
				return objectMapper.readValue(message, UserEntity.class);
			}

			@Override
			public boolean isEndOfStream(UserEntity user) {
				return false;
			}

			@Override
			public TypeInformation<UserEntity> getProducedType() {
				return TypeInformation.of(UserEntity.class);
			}
		}, props);
	}

	public static FlinkKafkaProducer011<UserEntity> createKafkaProducer(String topic, String kafkaAddress) {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", kafkaAddress);
		return new FlinkKafkaProducer011<>(kafkaAddress, topic, new SerializationSchema<UserEntity>() {
			private final ObjectMapper objectMapper = new ObjectMapper();

			@Override
			public byte[] serialize(UserEntity user) {
				try {
					byte[] bytes = objectMapper.writeValueAsBytes(user);
					System.out.println("Serialized User: " + new String(bytes)); // Log the serialized object
					return bytes;
				} catch (Exception e) {
					throw new RuntimeException("Failed to serialize User object", e);
				}
			}
		});
	}




}
