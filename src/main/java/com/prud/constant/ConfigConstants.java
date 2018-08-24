package com.prud.constant;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ConfigConstants {
	public static final String BANKOUT_TOPIC_NAME = "BankResponseIn";
	public static final String IL_TOPIC_NAME = "ILResponseIn";
	public static final String CONFIG_FILE_LOCATION = "C:\\D\\Prudential\\newWork\\consume\\FlinkKafkaConsumer\\BankConfig_acc_Collection1.xlsx";

	public static final String BOOTSTRAP_SERVER = "bootstrap.servers";
	public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
	public static final String GROUP_ID = "group.id";
	public static final String FLINK2_TOPIC = "TransformBankOut";
	public static final String TOPIC_NAME = "ILSourceRequestIn";
	public static final String BANKIN_TOPIC_NAME = "bankTopic";
	public static final String BOOTSTRAP_SERVER_CONFIG = "localhost:9092";
	public static final String ZOOKEEPER_CONFIG = "localhost:2181";
	public static final String GROUPID_CONFIG = "ilResponse";
	public static final String BANK_INPUT_FILE_NAME ="C:\\D\\Prudential\\newWork\\DropZone\\BANK_INPUT\\PT29040004"+new SimpleDateFormat("dd-MM-yy HH-mm-ss").format(new Date())+".DAT"; 
	public static final String IL_RESP_FILE_NAME ="C:\\D\\Prudential\\newWork\\DropZone\\IL_OUTPUT\\ILFlatFile"+new SimpleDateFormat("dd-MM-yy HH-mm-ss").format(new Date())+".txt"; 

}
