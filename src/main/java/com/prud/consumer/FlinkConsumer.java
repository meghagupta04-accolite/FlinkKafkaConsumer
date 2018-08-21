package com.prud.consumer;

import java.util.Properties;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import com.prud.constant.ConfigConstants;
/**
 * Class to read records from Kafka consumer and put those records into a file.
 *
 */
public class FlinkConsumer 
{
	public static void main( String[] args )
	{
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

		//set kafka and zookeeper properties
		Properties prop = new Properties();
		prop.setProperty(ConfigConstants.BOOTSTRAP_SERVER, ConfigConstants.BOOTSTRAP_SERVER_CONFIG);
		prop.setProperty(ConfigConstants.ZOOKEEPER_CONNECT, ConfigConstants.ZOOKEEPER_CONFIG);
		prop.setProperty(ConfigConstants.GROUP_ID, ConfigConstants.GROUPID_CONFIG);

		//Add Source to env stream
		DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer010<>(ConfigConstants.TOPIC__NAME, new SimpleStringSchema() {
		}, prop));


		/*KeyedStream<String, String> keyedEdits = messageStream.keyBy(new KeySelector<String, String>() {
			@Override
			public String getKey(String event) {
				return event;
			}
		});

		keyedEdits.window(TumblingEventTimeWindows.of(Time.seconds(5)))
		.reduce(new ReduceFunction<String>(){
			@Override
			public String reduce(String output, String input) throws Exception {
				output = input+"\n";
				return output;
			}
		});*/



		/*	DataStream<String> timestampStream = messageStream.rebalance().assignTimestampsAndWatermarks(timestampAndWatermarkAssigner)
				.assignTimestampsAndWatermarks(5);
		// Counts Strings
		timestampStream.timeWindowAll(Time.minutes(1)).reduce(new ReduceFunction<String>() {

			@Override
			public String reduce(String output, String input) throws Exception {
				output = input+"\n";
				return output;
			}
		});*/

		//write the stream into file
		messageStream.writeAsText(ConfigConstants.FILE_NAME);
		env.execute();
	}


	public static class SimpleStringSchema implements DeserializationSchema<String>{
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
