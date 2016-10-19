package com.dexels;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.replication.api.ReplicationMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class ReplicationMessageListSerde implements Serde<List<ReplicationMessage>> {

	private final ObjectMapper objectMapper = new ObjectMapper();
	
	private final static Logger logger = LoggerFactory.getLogger(ReplicationMessageListSerde.class);

	
	public ReplicationMessageListSerde() {
	}
	
	@Override
	public void close() {
		
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		
	}

	@Override
	public Deserializer<List<ReplicationMessage>> deserializer() {
		return new Deserializer<List<ReplicationMessage>>() {

			@Override
			public void close() {
			}

			@Override
			public void configure(Map<String, ?> config, boolean isKey) {
				
			}

			@Override
			public List<ReplicationMessage> deserialize(String topic, byte[] data) {
				if(data==null) {
					return Collections.emptyList();
				}
				return StreamOperators.parser.parseMessageList(data);
			}
		};
	}

	@Override
	public Serializer<List<ReplicationMessage>> serializer() {
		return new Serializer<List<ReplicationMessage>>() {

			@Override
			public void close() {
				
			}

			@Override
			public void configure(Map<String, ?> configs, boolean isKey) {
				
			}

			@Override
			public byte[] serialize(String topic, List<ReplicationMessage> data) {
				ArrayNode list = objectMapper.createArrayNode();
				data.stream().map(msg->msg.toJSON(objectMapper)).forEach(e->list.add(e));
				try {
					
					return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(list); //.wi.writeValueAsBytes(list);
				} catch (JsonProcessingException e) {
					logger.error("Error: ", e);
					// TODO, what is wise to return in this case?
					return null;
				}
			}
		};
	}

}
