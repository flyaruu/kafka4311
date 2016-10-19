package com.dexels;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessage.Operation;
import com.dexels.replication.api.ReplicationMessageParser;
import com.dexels.replication.factory.ReplicationFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class StreamOperators {
	
	public static final ReplicationMessageParser parser = ReplicationFactory.getDefaultInstance();
	public static final ReplicationMessageSerde replicationSerde = new ReplicationMessageSerde();
	public static final ReplicationMessageListSerde replicationListSerde = new ReplicationMessageListSerde();

	private static String tenant = "NHV";
	private static String deployment = "develop";
	


	
	private static ObjectMapper objectMapper = new ObjectMapper();
	
	private static ObjectWriter objectWriter = objectMapper.writer().withDefaultPrettyPrinter();

	
	private final static Logger logger = LoggerFactory.getLogger(StreamOperators.class);

//	
//	public static List<ReplicationMessage> addToReplicationList(String key, ReplicationMessage a, List<ReplicationMessage> b) {
//		List<ReplicationMessage> result = new LinkedList<>(b);
//		result.add(a);
//		return Collections.unmodifiableList(result);
//	}
//	
//	public static List<ReplicationMessage> removeFromReplicationList(String key, ReplicationMessage a, List<ReplicationMessage> b) {
//		List<ReplicationMessage> result = new LinkedList<>(b);
//		result.remove(a);
//		return Collections.unmodifiableList(result);
//	}
//	
	public static List<ReplicationMessage> addToReplicationList(List<ReplicationMessage> a, List<ReplicationMessage> b) {
		List<ReplicationMessage> result = new LinkedList<>(a);
		result.addAll(b);
		return Collections.unmodifiableList(result);
	}
	
	public static List<ReplicationMessage> removeFromReplicationList(List<ReplicationMessage> a, List<ReplicationMessage> b) {
		if(a==null) {
			return Collections.emptyList();
		}
		List<ReplicationMessage> result = new LinkedList<>(a);
		result.removeAll(b);
		return Collections.unmodifiableList(result);
	}
	

	public static byte[] writeObjectValue(ObjectNode node) {
		try {
			return objectWriter.writeValueAsBytes(node);
		} catch (JsonProcessingException e) {
			logger.error("Error: ", e);
			return new byte[]{};
		}
		
	}
	
	public static ReplicationMessage joinReplication(ReplicationMessage core, List<ReplicationMessage> added, String sub) {
		System.err.println("Merging with list: "+added);
		if(added!=null) {
			core.setSubMessages(sub, added);
		}
		return core;
	}

	public static ReplicationMessage joinReplicationSingle(ReplicationMessage core, ReplicationMessage added, String sub) {
		if(core==null) {
			System.err.println("No core. Thought this didn't happen");
			return null;
		}
		if(core.columnValue("latitude")!=null) {
			System.err.println("Wrong message as primary");
			return null;
		}

		
		if(added==null) {
			System.err.println("No added. ignoring");
			return core;
		}
		if(added.columnValue("addressid")!=null) {
			System.err.println("Wrong message as secondary");
			return added;
		}
		if(core.columnValue("addressid")==null) {
			System.err.println("No addressid?");
		}
		if(added.columnValue("addressid")!=null) {
			System.err.println("Addressid in secondary?!");
		}
		if(core.columnValue("latitude")!=null) {
			System.err.println("latitude in primary?!");
		}

		if("9671LP".equals(core.columnValue("zipcode"))) {
			System.err.println("zipcode");
		}
		if(added!=null && "9671LP".equals(added.columnValue("zipcode"))) {
			System.err.println("D");
		}
		System.err.println("Merging with single: "+added);
		if(added!=null) {
			core.setSubMessage(sub, added);
		}
		if(core.columnValue("addressid")==null) {
			System.err.println("WEIRD: \n"+new String(core.toBytes()));
		}
		return core;
	}

	public static ReplicationMessage merge(String key, ReplicationMessage a, ReplicationMessage b) {
		if(b==null) {
			return a;
		}
		return parser.join(key, a, b);
	}

	public static ReplicationMessage joinField(ReplicationMessage a, ReplicationMessage b, String keyField, String valueField) {
		if(b==null) {
			return a;
		}
		String key = (String) b.columnValue(keyField);
		Object val = b.columnValue(valueField);
		String type = b.columnType(valueField);
		return parser.merge(a,key,val,type);
	}

	public static ReplicationMessage joinFieldList(ReplicationMessage a, List<ReplicationMessage> joinList, String keyField, String valueField) {
		if(joinList==null) {
			return a;
		}
		ReplicationMessage current = a;
		for (ReplicationMessage replicationMessage : joinList) {
			String key = (String) replicationMessage.columnValue(keyField);
			key = key.toLowerCase(); // seems to be the standard
			Object val = replicationMessage.columnValue(valueField);
			String type = replicationMessage.columnType(valueField);
			current = parser.merge(current,key,val,type);			
		}
		return current;
	}
	public static KTable<String, ReplicationMessage> tableFrom(KStreamBuilder builder, String topic, String name) {
		KTable<String, ReplicationMessage> teamAttributes = builder.table(Serdes.String(),StreamOperators.replicationSerde,topic,name)
	    		.filter((key,message)->message.operation()!=Operation.COMMIT)
	    		.filter((key,value)->key!=null && value!=null)
	    		.mapValues((msg)->msg.operation()==Operation.DELETE?null:msg);
		return teamAttributes;
	}
	
	public static KStream<String, ReplicationMessage> streamFrom(KStreamBuilder builder, String name) {
		KStream<String, ReplicationMessage> stream = builder.stream(Serdes.String(),StreamOperators.replicationSerde,name)
	    		.filter((key,value)->key!=null && value!=null)
	    		.filter((key,message)->message.operation()!=Operation.COMMIT);
		return stream;
	}
	
	public static KTable<String, List<ReplicationMessage>> documentsOfType(KTable<String, ReplicationMessage> documentTable,String type) {
//		KTable<String, ReplicationMessage> filtered = documentTable.filter((documentid, message)->{
//			return type.equals(message.columnValue("objecttype")) && message.columnValue("data")!=null;
//		});
//		return StreamOperators.merge(builder, filtered, "_document_"+type,msg->extractKey(msg));
		return mergedocuments("objectid", "_document_"+type,documentTable,type);
	}

	
	private static KTable<String, List<ReplicationMessage>> mergedocuments(String entityId,String prefix, KTable<String, ReplicationMessage> documentTable,String type) {
		return documentTable.filter((id, message)->type.equals(message.columnValue("objecttype")) && message.columnValue("data")!=null)
			.filter((k,v)->v!=null)
    		.groupBy((k,v)->new KeyValue<>((String)v.columnValue(entityId),Arrays.asList(v)),Serdes.String() , StreamOperators.replicationListSerde)
			.reduce((a,b)->StreamOperators.addToReplicationList(a,b), (a,b)->StreamOperators.removeFromReplicationList(a,b), prefix)
			.through(Serdes.String(), replicationListSerde, prefix+"_merge_"+type, prefix+"_mergetopic_"+type);
	}

	public static KTable<String, List<ReplicationMessage>> mergeToList(KStreamBuilder builder, String entityId,String prefix, KTable<String, ReplicationMessage> joinDataStream,KTable<String, ReplicationMessage> entityJoin) {
		return entityJoin.join(joinDataStream, (m1,m2)->StreamOperators.merge(entityId, m1, m2))
	    		.filter((k,v)->v!=null)
	    		.groupBy((k,v)->new KeyValue<>((String)v.columnValue(entityId),Arrays.asList(v)),Serdes.String() , StreamOperators.replicationListSerde)
				.reduce((a,b)->StreamOperators.addToReplicationList(a,b), (a,b)->StreamOperators.removeFromReplicationList(a,b), prefix);
	}

	public static KTable<String, List<ReplicationMessage>> mergeToList(KStreamBuilder builder, String entityId,String prefix, KTable<String, ReplicationMessage> stream) {
		return stream
	    		.filter((k,v)->v!=null)
	    		.groupBy((k,v)->new KeyValue<>((String)v.columnValue(entityId),Arrays.asList(v)),Serdes.String() , StreamOperators.replicationListSerde)
				.reduce((a,b)->StreamOperators.addToReplicationList(a,b), (a,b)->StreamOperators.removeFromReplicationList(a,b), prefix);
	}

	
	private static String extractKey(ReplicationMessage msg) {
		String id = (String)(msg.columnValue("objectid"));
		return id;
	}
	
	public static KTable<String, ReplicationMessage> replicationTable(KStreamBuilder builder, String name) {
    	return tableFrom(builder,replicationTopicName(name),replicationTopicName(name));
    }

    public static KStream<String, ReplicationMessage> replicationStream(KStreamBuilder builder, String name) {
    	return streamFrom(builder,replicationTopicName(name));
    }

	protected static String replicationTopicName(String name) {
		return "REPLICATION-"+tenant+"-"+deployment+"-"+name;
	}
    
    public static String internalTopicName(String name) {
    	return tenant+"-"+deployment+"-"+name;
    }
    
    public static KTable<String, ReplicationMessage> internalTable(KStreamBuilder builder, String name) {
    	return tableFrom(builder,internalTopicName(name),replicationTopicName(name));
    }

    public static KStream<String, ReplicationMessage> internalStream(KStreamBuilder builder, String name) {
    	return streamFrom(builder,internalTopicName(name));
    }

}
