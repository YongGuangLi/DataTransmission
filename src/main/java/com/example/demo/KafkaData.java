package com.example.demo;

public class KafkaData {
	int mainFlag;
	
	public int getMainFlag() {
		return mainFlag;
	}
	String topic;
	public String getTopic() {
		return topic;
	}
	String value;
	public String getValue() {
		return value;
	}
	
	KafkaData(int mainFlag, String topic, String value){
		this.mainFlag = mainFlag;
		this.topic = topic;
		this.value = value;
	}
}
