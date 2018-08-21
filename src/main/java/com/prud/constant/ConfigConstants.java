package com.prud.constant;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ConfigConstants {
	public static final String BOOTSTRAP_SERVER = "bootstrap.servers";
	public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
	public static final String GROUP_ID = "group.id";
	public static final String TOPIC__NAME = "ILSourceRequestIn";
	public static final String BOOTSTRAP_SERVER_CONFIG = "localhost:9092";
	public static final String ZOOKEEPER_CONFIG = "localhost:3180";
	public static final String GROUPID_CONFIG = "ilResponse";
	public static final String FILE_NAME ="ILResponse"+new SimpleDateFormat("dd-MM-yy HH-mm-ss").format(new Date())+".txt"; 
}
