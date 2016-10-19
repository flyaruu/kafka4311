package com.dexels;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.dexels.replication.api.ReplicationMessage;

public class ReplicationMessageSerde implements Serde<ReplicationMessage> {


	public ReplicationMessageSerde() {
	}
	
	@Override
	public void close() {
		
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		
	}

	@Override
	public Deserializer<ReplicationMessage> deserializer() {
		return new Deserializer<ReplicationMessage>() {

			@Override
			public void close() {
			}

			@Override
			public void configure(Map<String, ?> config, boolean isKey) {
				
			}

			@Override
			public ReplicationMessage deserialize(String topic, byte[] data) {
				return StreamOperators.parser.parseBytes(data);
			}
		};
	}

	@Override
	public Serializer<ReplicationMessage> serializer() {
		return new Serializer<ReplicationMessage>() {

			@Override
			public void close() {
				
			}

			@Override
			public void configure(Map<String, ?> configs, boolean isKey) {
				
			}

			@Override
			public byte[] serialize(String topic, ReplicationMessage data) {
				if(data==null) {
					return null;
				}
				return data.toBytes();
			}
		};
	}

}
