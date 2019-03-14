package com.example.demo;
  
 
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;  
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class RedisListenerConfig { 

	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Bean
    RedisMessageListenerContainer container(RedisConnectionFactory connectionFactory,MessageListenerAdapter listenerAdapter) {
 
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(listenerAdapter, new PatternTopic("DataTransmission"));   //订阅topic   
		
        return container;
    }
 
	@Bean
    MessageListenerAdapter listenerAdapter() {
		RedisMessageListener redisMessageListener = new RedisMessageListener();  
		redisMessageListener.getStationConnectConfig(jdbcTemplate);               //也可通过RedisMessageListener构造函数传入
        return new MessageListenerAdapter(redisMessageListener);
    }
	
}
