import java.util.Properties;  
  
import kafka.javaapi.producer.Producer;  
import kafka.producer.KeyedMessage;  
import kafka.producer.ProducerConfig;  

public class KafkaProducer {
	
	private Producer<String, String> producer;
	public static String TOPIC = "test";
	
	public KafkaProducer() {
		Properties props = new Properties();
		// 此处配置的是kafka的端口 
		props.put("metadata.broker.list", "127.0.0.1:9092");
		props.put("zk.connect", "127.0.0.1:2181");
		
		// 配置value的序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// 配置key的序列化类
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		
		props.put("request.required.acks", "-1");
		
		producer = new Producer<String,String>(new ProducerConfig(props));
	}
	
	public void producer() {
		int i;
		i = 10;
		String key,data;
		while(i < 100){
			key = String.valueOf(i);
			data = "this is " + key;
			producer.send(new KeyedMessage<String, String>(TOPIC, key,data));
			System.out.println(data);
			i++;
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		new KafkaProducer().producer();
	}

}
