package com.dexels;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessage.Operation;

public class KafkaTest {
	private final static Logger logger = LoggerFactory.getLogger(KafkaTest.class);
	
	public static void main(String[] args) {
		processSyntheticData("streamtest17");
//		processRealData("streamtest16");
	}

	private static void processSyntheticData(String applicationName) {
		KStreamBuilder builder = new KStreamBuilder();
		KTable<String,ReplicationMessage> personaddress = builder.table(Serdes.String(),StreamOperators.replicationSerde,"TESTPERSONADDRESS","TESTPERSONADDRESS")
    		.filter((key,message)->message.operation()!=Operation.COMMIT)
    		.filter((key,value)->key!=null && value!=null);

		KTable<String,ReplicationMessage> address = builder.table(Serdes.String(),StreamOperators.replicationSerde,"TESTADDRESS","TESTADDRESS")
    		.filter((key,message)->message.operation()!=Operation.COMMIT)
    		.filter((key,value)->key!=null && value!=null);

		builder.table(Serdes.String(),StreamOperators.replicationSerde,"TESTPERSON","TESTPERSON")
    		.filter((key,message)->message.operation()!=Operation.COMMIT)
    		.filter((key,value)->key!=null && value!=null)
    		.mapValues((msg)->msg.operation()==Operation.DELETE?null:msg)
			.leftJoin(mergeToList(builder,"personid","addr-person", address, personaddress), (core,added)->StreamOperators.joinReplication(core,added,"addresses"))
//			.to(Serdes.String(), StreamOperators.replicationSerde,"PERSON")
			.mapValues(msg->StreamOperators.parser.toFlatJson(msg))
			.mapValues(on->StreamOperators.writeObjectValue(on))
			.to(Serdes.String(), Serdes.ByteArray(), StreamOperators.internalTopicName("FLATPERSON"));
		startStream(builder, applicationName);
	}
	

	private static void processRealData(String applicationName) {
		KStreamBuilder builder = new KStreamBuilder();
		KTable<String,ReplicationMessage> personaddress = builder.table(Serdes.String(),StreamOperators.replicationSerde,"REPLICATION-NHV-develop-sportlinkkernel-PERSONADDRESS","REPLICATION-NHV-develop-sportlinkkernel-PERSONADDRESS")
	    		.filter((key,message)->message.operation()!=Operation.COMMIT)
	    		.filter((key,value)->key!=null && value!=null);

		KTable<String,ReplicationMessage> address = builder.table(Serdes.String(),StreamOperators.replicationSerde,"REPLICATION-NHV-develop-sportlinkkernel-ADDRESS","REPLICATION-NHV-develop-sportlinkkernel-ADDRESS")
	    		.filter((key,message)->message.operation()!=Operation.COMMIT)
	    		.filter((key,value)->key!=null && value!=null);

		builder.table(Serdes.String(),StreamOperators.replicationSerde,"REPLICATION-NHV-develop-sportlinkkernel-PERSON","REPLICATION-NHV-develop-sportlinkkernel-PERSON")
	    		.filter((key,message)->message.operation()!=Operation.COMMIT)
	    		.filter((key,value)->key!=null && value!=null)
	    		.mapValues((msg)->msg.operation()==Operation.DELETE?null:msg)
				.leftJoin(mergeToList(builder,"personid","addr-person", address, personaddress), (core,added)->StreamOperators.joinReplication(core,added,"addresses"))
//				.to(Serdes.String(), StreamOperators.replicationSerde,"PERSON")
				.mapValues(msg->StreamOperators.parser.toFlatJson(msg))
				.mapValues(on->StreamOperators.writeObjectValue(on))
				.to(Serdes.String(), Serdes.ByteArray(), StreamOperators.internalTopicName("FLATJSON"));
		startStream(builder, applicationName);
	}

	
	protected static void startStream(KStreamBuilder builder,String name) {
		Properties streamsConfiguration = createProperties(name);
		KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
	    streams.start();		
	}
	
    private static Properties createProperties(String applicationName) {
		Properties streamsConfiguration = new Properties();
	    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
	    // against which the application is run.
		logger.info("Creating application with name: {}",applicationName);
	    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName);
	    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.0.1:9092,10.0.0.1:9093,10.0.0.1:9094");
	    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "10.0.0.1:2181");
	    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
	    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, StreamOperators.replicationSerde.getClass().getName());
	    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	    streamsConfiguration.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
	    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, System.getProperty("user.dir")+"/kafka-streams");
	    streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 10);
//	    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		return streamsConfiguration;
	}
	public static KTable<String, List<ReplicationMessage>> mergeToList(KStreamBuilder builder, String entityId,String prefix, KTable<String, ReplicationMessage> joinDataStream,KTable<String, ReplicationMessage> entityJoin) {
		return entityJoin.join(joinDataStream, (m1,m2)->StreamOperators.merge(entityId, m1, m2))
	    		.filter((k,v)->v!=null)
	    		.groupBy((k,v)->new KeyValue<>((String)v.columnValue(entityId),Arrays.asList(v)),Serdes.String() , StreamOperators.replicationListSerde)
				.reduce((a,b)->StreamOperators.addToReplicationList(a,b), (a,b)->StreamOperators.removeFromReplicationList(a,b), prefix);
	}

}