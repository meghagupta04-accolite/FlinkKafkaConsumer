package com.pru.africa.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

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
		prop.setProperty(Constants.BOOTSTRAP_SERVER, Constants.BOOTSTRAP_SERVER_CONFIG);
		prop.setProperty(Constants.ZOOKEEPER_CONNECT, Constants.ZOOKEEPER_CONFIG);
		prop.setProperty(Constants.GROUP_ID, Constants.GROUPID_CONFIG);

		//Add Source to env stream
		DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer082<>(Constants.TOPIC__NAME, new SimpleStringSchema(), prop));

		/*//set window time to 2.5sec
		messageStream.window(Time.of(2500, TimeUnit.MILLISECONDS));
		*/
		//write the stream into file
		messageStream.writeAsText(Constants.FILE_NAME).setParallelism(1);
		env.execute();
	}
}
