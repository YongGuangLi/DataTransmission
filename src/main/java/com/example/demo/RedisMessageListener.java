package com.example.demo;
 
 
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map; 
 
import org.apache.log4j.Logger; 
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener; 
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.protobuf.InvalidProtocolBufferException;
import com.networkcollect.NetworkCollect;

public class RedisMessageListener extends Thread implements MessageListener {

	private static Logger logger = Logger.getLogger(RedisMessageListener.class);
	private KafkaProducerThread kafkaProducerThread; 
	private JdbcTemplate jdbcTemplate;
	private Map<String, String> mapDataChannelServers;
	private Map<String, Integer> mapDataChannelMainFlag;
	
	public RedisMessageListener(JdbcTemplate jdbcTemplate) {
		mapDataChannelServers = new HashMap<String, String>();
		mapDataChannelMainFlag = new HashMap<String, Integer>();
		kafkaProducerThread = new KafkaProducerThread();
		kafkaProducerThread.start();  
		this.jdbcTemplate = jdbcTemplate;
		getStationConnectConfig();
	}

	String getServersByDataChannelName(String dataChannelNames, String dataChannelPort){
		String servers = null;
		for (String dataChannelName : dataChannelNames.split(",")) {  
			if (servers == null)  
				servers = String.format("%s:%s", dataChannelName, dataChannelPort); 
			else  
				servers += String.format(",%s:%s", dataChannelName, dataChannelPort);  
		}
		return servers;
	}
	
	void updateDataChannelStatus(int status, String connId){
		String updateSqlp = String.format("update station_connect_config set DataChannelStatus = %d where Id = '%s'", status, connId);
		jdbcTemplate.execute(updateSqlp);
	}
	public void getStationConnectConfig() { 
		String sql = "select Id,DataChannelName,DataChannelPort,MainFlag from station_connect_config";
		List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
		for (Map<String, Object> map : list) {
			
			String connId = (String) map.get("Id");
			String dataChannelNames = (String) map.get("DataChannelName");
			String dataChannelPort = (String) map.get("DataChannelPort");
			int mainFlag = Integer.parseInt((String) map.get("MainFlag")); 
			
			String servers = getServersByDataChannelName(dataChannelNames, dataChannelPort);
			
			logger.info(String.format("%s %s %d", connId, servers , mainFlag)); 
			mapDataChannelServers.put(connId, servers);
			mapDataChannelMainFlag.put(connId, mainFlag);
			
			kafkaProducerThread.CreateKafkaConnect(connId,servers, mainFlag);  
		}
	}
	
	@Override
	public void onMessage(Message message, byte[] pattern) {
		NetworkCollect.MainMessage mainMessage;
		try {
			mainMessage = NetworkCollect.MainMessage.parseFrom(message.getBody());
		
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
				String dataChannelNames = stationConnectConfig.getDataChannelName();
				String dataChannelPort = stationConnectConfig.getDataChannelPort();
				int mainFlag = stationConnectConfig.getMainFlag();
				NetworkCollect.RemoteSetType remoteSetType = stationConnectConfig.getSetType();
				
				String servers = getServersByDataChannelName(dataChannelNames, dataChannelPort);
				
				logger.info(String.format("%s %s %d %s", connId, servers , mainFlag, remoteSetType)); 
				switch (remoteSetType) {
				case RST_ADD:
					kafkaProducerThread.CreateKafkaConnect(connId,servers, mainFlag);
					mapDataChannelServers.put(connId, servers);
					mapDataChannelMainFlag.put(connId, mainFlag);
					break;
				case RST_MODIFY:
					kafkaProducerThread.ModifyKafkaConnect(connId,servers, mainFlag);
					mapDataChannelServers.put(connId, servers);
					mapDataChannelMainFlag.put(connId, mainFlag);
					break;
				case RST_DELETE:
					kafkaProducerThread.DeleteKafkaConnect(connId, mainFlag);
					mapDataChannelServers.remove(connId);
					mapDataChannelMainFlag.remove(connId);
					break;
				default:
					break;
				}
				break; 
			default:
				break;
			}
		} catch (InvalidProtocolBufferException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	public void run() {
		while (!this.isInterrupted()) {
			for (String connId : mapDataChannelServers.keySet()) {
				String servers = mapDataChannelServers.get(connId);
				int mainFlag = mapDataChannelMainFlag.get(connId);
				
				Socket client = null;
				try{
					String[] strLiStrings = servers.split(":"); 
					client = new Socket(strLiStrings[0], Integer.parseInt(strLiStrings[1]));
					System.out.println("端口已开放" + servers);
					client.close(); 
					if(kafkaProducerThread.isContainsKafkaConnect(connId, mainFlag) == false){ 
						kafkaProducerThread.CreateKafkaConnect(connId,servers, mainFlag);
					}

					updateDataChannelStatus(1, connId);
				}catch(Exception e){
					System.out.println("端口未开放:" + servers);
					kafkaProducerThread.DeleteKafkaConnect(connId, mainFlag); 
					updateDataChannelStatus(0, connId); 
				} 
			}
			
			try {
				sleep(10 * 1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} 
	}
}
