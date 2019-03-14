package com.example.demo;
 
 
import java.util.List;
import java.util.Map; 

import org.apache.log4j.Logger; 
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener; 
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.protobuf.InvalidProtocolBufferException;
import com.networkcollect.NetworkCollect;

public class RedisMessageListener implements MessageListener {

	private static Logger logger = Logger.getLogger(RedisMessageListener.class);
	private KafkaProducerThread kafkaProducerThread; 
	
	public RedisMessageListener() {
		kafkaProducerThread = new KafkaProducerThread();
		kafkaProducerThread.start();  
	}

	public void getStationConnectConfig(JdbcTemplate jdbcTemplate) { 
		String sql = "select Id,DataChannelName,DataChannelPort,MainFlag from station_connect_config";
		List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
		for (Map<String, Object> map : list) {
			
			String connId = (String) map.get("Id");
			String dataChannelName = (String) map.get("DataChannelName");
			String dataChannelPort = (String) map.get("DataChannelPort");
			int mainFlag = Integer.parseInt((String) map.get("MainFlag")); 
			
			logger.info(String.format("%s %s %s %d", connId, dataChannelName, dataChannelPort, mainFlag)); 
			
			kafkaProducerThread.CreateKafkaConnect(connId,dataChannelName, dataChannelPort, mainFlag); 
		}
	}
	
	@Override
	public void onMessage(Message message, byte[] pattern) {
		try {
			NetworkCollect.MainMessage mainMessage = NetworkCollect.MainMessage.parseFrom(message.getBody());
			switch (mainMessage.getMsgType()) {
			case MT_CollectData:
				NetworkCollect.CollectData collectData = mainMessage.getCollectData();
				String itemValueString = collectData.getItemValue();
				try {
					kafkaProducerThread.addKafkaDate(new KafkaData(0, "Event", itemValueString));
				} catch (InterruptedException e) { 
					e.printStackTrace();
				}
				logger.debug("CollectData:" + itemValueString);
				break;
			case MT_CommunicationPair:
				NetworkCollect.CommunicationPair communicationPair = mainMessage.getCommunicationPair();
				String communicationPairLog = communicationPair.getProtocol();
				try {
					kafkaProducerThread.addKafkaDate(new KafkaData(0, "CommPair", communicationPairLog));
				} catch (InterruptedException e) { 
					e.printStackTrace();
				}
				logger.debug("CommunicationPair:" + communicationPairLog);
				break;
			case MT_OriginalSyslog:
				NetworkCollect.OriginalSyslog originalSyslog = mainMessage.getOriginalSyslog();
				String originalSyslogLog = originalSyslog.getLogDetail();
				try {
					kafkaProducerThread.addKafkaDate(new KafkaData(0, "OriginalSyslog", originalSyslogLog));
				} catch (InterruptedException e) { 
					e.printStackTrace();
				}
				logger.debug("OriginalSyslog:" + originalSyslogLog);
				break;
			case MT_NMAP:
				NetworkCollect.NmapInfo nmapInfo = mainMessage.getNmapInfo();
				String nmapLog = nmapInfo.getLogDetail();
				try {
					kafkaProducerThread.addKafkaDate(new KafkaData(0, "Nmap",nmapLog));
				} catch (InterruptedException e) { 
					e.printStackTrace();
				}
				logger.debug("NmapInfo:" + nmapLog);
				break;
			case MT_RESTOREFILE:
				NetworkCollect.RestoreFile restoreFile = mainMessage.getRestoreFile();
				String restoreFileLog = restoreFile.getLogDetail();
				try {
					kafkaProducerThread.addKafkaDate(new KafkaData(0, "UnidentifiedFile", restoreFileLog));
				} catch (InterruptedException e) { 
					e.printStackTrace();
				}
				logger.debug("RestoreFile:" + restoreFileLog);
				break;
			case MT_SubMainStationData:
				NetworkCollect.SubMainStationData subMainStationData = mainMessage.getSubMainStationData();
				String dataStr = subMainStationData.getDataStr();
				try {
					kafkaProducerThread.addKafkaDate(new KafkaData(1, "Event",dataStr));
				} catch (InterruptedException e) { 
					e.printStackTrace();
				}
				logger.debug("SubMainStationData:" + dataStr);
				break;
			case MT_StationConnectConfig:
				NetworkCollect.StationConnectConfig stationConnectConfig = mainMessage.getStationConnectConfig();
				String connId = stationConnectConfig.getId();
				String dataChannelIp = stationConnectConfig.getDataChannelIp();
				String dataChannelName = stationConnectConfig.getDataChannelName();
				String dataChannelPort = stationConnectConfig.getDataChannelPort();
				int mainFlag = stationConnectConfig.getMainFlag();
				NetworkCollect.RemoteSetType remoteSetType = stationConnectConfig.getSetType();

				logger.info(String.format("%s %s %s %s %s", connId, dataChannelIp, dataChannelName, dataChannelPort, remoteSetType.toString()));

				switch (remoteSetType) {
				case RST_ADD:
					kafkaProducerThread.CreateKafkaConnect(connId,dataChannelName, dataChannelPort, mainFlag);
					break;
				case RST_MODIFY:
					kafkaProducerThread.ModifyKafkaConnect(connId,dataChannelName, dataChannelPort, mainFlag);
					break;
				case RST_DELETE:
					kafkaProducerThread.DeleteKafkaConnect(connId, mainFlag);
					break;
				default:
					break;
				}
				break;
			default:
				break;
			}
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
	}
}
