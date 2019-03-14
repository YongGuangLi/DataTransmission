package com.example.demo;
 
  
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext; 

@SpringBootApplication
public class DataTransmission {

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		ConfigurableApplicationContext contex = SpringApplication.run(DataTransmission.class, args); 
		  
		synchronized (DataTransmission.class) {
			while (true) {
				try {
					DataTransmission.class.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}

 


//@SuppressWarnings("rawtypes")
//RedisTemplate template = (RedisTemplate) contex.getBean("redisTemplate"); 
// template.convertAndSend("chat", "Hello from Redis!"); //发布

//@Autowired
//private JdbcTemplate jdbcTemplate;

//String sql = "select * from station_connect_config";
//List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
//for (Map<String, Object> map : list) {
//	Set<Entry<String, Object>> entries = map.entrySet();
//	if (entries != null) {
//		Iterator<Entry<String, Object>> iterator = entries.iterator();
//		while (iterator.hasNext()) {
//			Entry<String, Object> entry = (Entry<String, Object>) iterator.next();
//			Object key = entry.getKey();
//			Object value = entry.getValue();
//			System.out.println(key + ":" + value);
//		}
//	}
//}
