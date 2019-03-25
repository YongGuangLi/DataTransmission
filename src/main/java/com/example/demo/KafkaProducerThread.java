package com.example.demo;
 
import java.util.HashMap; 
import java.util.Map;
import java.util.Properties; 
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger; 

 

public class KafkaProducerThread extends Thread { 
	
	private static Logger logger = Logger.getLogger(KafkaProducerThread.class);
	
	private Map<String, KafkaProducer<String, String>> mapMainKafkaProducer;
	private Map<String, KafkaProducer<String, String>> mapSubKafkaProducer;
	private LinkedBlockingQueue<KafkaData> queueKafkaData; 
	
	public KafkaProducerThread(){
		mapMainKafkaProducer = new HashMap<String, KafkaProducer<String, String>>();
		mapSubKafkaProducer = new HashMap<String, KafkaProducer<String, String>>();
		queueKafkaData = new LinkedBlockingQueue<KafkaData>(); 
	} 
	
	//flag:0-主主站  flag:1-附属主站
	public void CreateKafkaConnect(String connId,String servers, int flag) { 
		Properties props = new Properties();
		props.put("bootstrap.servers", servers); 
		props.put("acks", "all");
		props.put("retries", 2);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);  
		props.put("max.request.size", 10485760);  
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String, String> producer = new KafkaProducer<>(props);

		synchronized (KafkaProducerThread.class) {
			if (flag == 0) { 
				mapMainKafkaProducer.put(connId, producer);
			} else if(flag == 1) {
				mapSubKafkaProducer.put(connId, producer);
			}
		}
	}
	
	public void ModifyKafkaConnect(String connId,String servers, int flag){
		DeleteKafkaConnect(connId, flag);
		CreateKafkaConnect(connId, servers, flag);
	}
	
	public void DeleteKafkaConnect(String connId,int flag){
		synchronized (KafkaProducerThread.class) {
			if (flag == 0) { 
				if(mapMainKafkaProducer.containsKey(connId))
				{ 
					KafkaProducer<String, String> producer = mapMainKafkaProducer.remove(connId);
					producer.close();
				}
			} else if(flag == 1) { 
				if(mapSubKafkaProducer.containsKey(connId))
				{ 
					KafkaProducer<String, String> producer = mapSubKafkaProducer.remove(connId);
					producer.close();
				} 
			}
		}
	}
	
	public void addKafkaDate(KafkaData kafkaData) throws InterruptedException{
		queueKafkaData.put(kafkaData);
	}
	
	static class ProducerAckCallback implements Callback { 
		private final String topic; 
		private final String value;
 
		public ProducerAckCallback(String topic, String value) { 
			this.topic = topic;
			this.value = value;
		}
 
		@Override
		public void onCompletion(RecordMetadata metadata, Exception e) { 
			if (null == metadata) {
				logger.warn(("主题:" + topic + " 消息:" + value + " 发送失败!" + e.getMessage()));
			} 
			//logger.debug("主题:" + topic  + ")send to partition(" + metadata.partition()+ " and offest " + metadata.offset());
		}
	} 
 
	public void run() {
		while (!this.isInterrupted()) {
			KafkaData kafkaData = null;
			try {
				kafkaData = queueKafkaData.take();
			} catch (InterruptedException e) { 
				e.printStackTrace();
			}
			int mainFlag = kafkaData.getMainFlag();
			String topic = kafkaData.getTopic();
			String value= kafkaData.getValue(); 
 
			synchronized (KafkaProducerThread.class) {
				if (mainFlag == 0) {
					for (KafkaProducer<String, String> producer : mapMainKafkaProducer.values()) {   
						producer.send(new ProducerRecord<String, String>(topic, value), new ProducerAckCallback(topic, value));
					}
				} else if (mainFlag == 1){
					for (KafkaProducer<String, String> producer : mapSubKafkaProducer.values()) {   
						producer.send(new ProducerRecord<String, String>(topic, value), new ProducerAckCallback(topic, value));
					}
				}
			}
		}
	}  
}
