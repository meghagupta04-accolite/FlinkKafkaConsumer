package com.prud.consumer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import com.prud.constant.ConfigConstants;

/**
 * Class to read records from Kafka consumer and put those records into a file.
 *
 */
public class RespFlinkBankConsumer {
	public static void main(String[] args) {
		try {
			kafkaReader();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void kafkaReader() throws Exception {
		// create execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// set kafka and zookeeper properties
		Properties prop = new Properties();
		prop.setProperty(ConfigConstants.BOOTSTRAP_SERVER, ConfigConstants.BOOTSTRAP_SERVER_CONFIG);
		prop.setProperty(ConfigConstants.ZOOKEEPER_CONNECT, ConfigConstants.ZOOKEEPER_CONFIG);
		prop.setProperty(ConfigConstants.GROUP_ID, ConfigConstants.GROUPID_CONFIG);

		FlinkKafkaConsumer010<String> flinkKafkaConsumer = new FlinkKafkaConsumer010<>(ConfigConstants.BANKOUT_TOPIC_NAME,new SimpleStringSchema(), prop);

		DataStream<String> messageStream = env.addSource(flinkKafkaConsumer);
		
		messageStream.print();
		
		/*add configurator jar*/
		
		

		messageStream.addSink(new FlinkKafkaProducer011<String>(ConfigConstants.BOOTSTRAP_SERVER_CONFIG,ConfigConstants.IL_TOPIC_NAME, new SimpleStringSchema()));
		
		env.execute();
	}

	public static class SimpleStringSchema implements DeserializationSchema<String>, SerializationSchema<String> {
		private static final long serialVersionUID = 1L;

		public SimpleStringSchema() {
		}

		public String deserialize(byte[] message) {
			return new String(message);
		}

		public boolean isEndOfStream(String nextElement) {
			return false;
		}

		public byte[] serialize(String element) {
			return element.getBytes();
		}

		public TypeInformation<String> getProducedType() {
			return TypeExtractor.getForClass(String.class);
		}

	}
}
