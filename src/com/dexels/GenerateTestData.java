package com.dexels;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessageParser;
import com.dexels.replication.api.ReplicationMessage.Operation;
import com.dexels.replication.impl.ReplicationMessageParserImpl;

public class GenerateTestData {
	
	private static KafkaProducer<String,byte[]> producer = null;
	private static ReplicationMessageParser parser  = new ReplicationMessageParserImpl();
	private static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	private static SecureRandom rnd = new SecureRandom();

	public static void activate() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "10.0.0.1:9092,10.0.0.1:9093,10.0.0.1:9094");
		props.put("acks", "all");
		props.put("batch.size", 16384);
		props.put("request.timeout.ms", 30000);
		props.put("compression.type", "lz4");
		
		props.put("linger.ms", 50);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", StringSerializer.class.getCanonicalName());
		props.put("value.serializer", ByteArraySerializer.class.getCanonicalName());
		props.put("block.on.buffer.full", true);
		props.put("retries", 100);
		producer  = new KafkaProducer<>(props);
	}
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		activate();
		for (int i = 0; i < 50000; i++) {
			ReplicationMessage rm = createReplicationMessage("personid","PERSON"+i,"name","name"+i,"gender","gender"+i,20);
//			String res = new String( rm.toBytes());
//			System.err.println("res: "+res);
			ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<String, byte[]>("TESTPERSON","PERSON"+i, rm.toBytes());
			producer.send(producerRecord);
		}
		for (int i = 90000; i >= 0; i--) {
			ReplicationMessage rm = createReplicationMessage("addressid","ADDRESS"+i,"street","street"+i,"city","city"+i,20);
//			String res = new String( rm.toBytes());
//			System.err.println("res: "+res);
			ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<String, byte[]>("TESTADDRESS","ADDRESS"+i, rm.toBytes());
			producer.send(producerRecord);
		}
		for (int i = 0; i < 150000; i++) {
			ReplicationMessage rm = createReplicationMessage("addressid","ADDRESS"+i,"personid","PERSON"+(int)(i / 10),"other","other"+i,20);
//			String res = new String( rm.toBytes());
//			System.err.println("res: "+res);
			ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<String, byte[]>("TESTPERSONADDRESS","ADDRESS"+i, rm.toBytes());
			producer.send(producerRecord);
		}	
		
		producer.flush();
		producer.close();
		System.err.println("done");
	}

	private static ReplicationMessage createReplicationMessage(String primary, String primaryValue, String key1,String value1, String key2,String value2, int numberOfRandomFields) {
		Map<String,Object> replication = new HashMap<>();
		Map<String,Object> data = new HashMap<>();
		data.put(primary,primaryValue);
		data.put(key1,value1);
		data.put(key2,value2);
		for (int i = 0; i < numberOfRandomFields; i++) {
			data.put("randomField"+i, randomString(30));
		}
		replication.put("Columns", data);
		List<String> keys = new LinkedList<>();
		keys.add(primary);

		replication.put("Timestamp", System.currentTimeMillis());
		replication.put("Operation", Operation.UPDATE.toString());
		replication.put("PrimaryKeys", keys);
		ReplicationMessage rm = parser.parseMap(replication);
		return rm;
	}



	private static String randomString( int len ){
	   StringBuilder sb = new StringBuilder( len );
	   for( int i = 0; i < len; i++ ) 
	      sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
	   return sb.toString();
	}
}
