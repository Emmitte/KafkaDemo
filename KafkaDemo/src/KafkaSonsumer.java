import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerConnector;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

public class KafkaSonsumer {
	
	private kafka.javaapi.consumer.ConsumerConnector consumer;
	
	public KafkaSonsumer() {
		Properties props = new Properties();
		// zookeeper 配置
		props.put("zookeeper.connect", "127.0.0.1:2181");
		
		// group 代表一个消费组
		props.put("group.id", "testGroup");
		
		// zk连接超时
		props.put("zookeeper.session.timeout.ms", "4000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("rebalance.max.retries", "5");
		props.put("rebalance.backoff.ms", "1200");
		
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "smallest");
		
		// 序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		
		ConsumerConfig config = new ConsumerConfig(props);
		
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
	}
	
	public void consume() {
		Map<String, Integer> topicCountMap = new HashMap<>();
		topicCountMap.put(KafkaProducer.TOPIC, new Integer(1));
		
		StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
		StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
		
		Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
		KafkaStream<String, String> stream = consumerMap.get(KafkaProducer.TOPIC).get(0);
		ConsumerIterator<String, String> iter = stream.iterator();
		while(iter.hasNext()){
			System.out.println(iter.next().message());
		}
	}

	public static void main(String[] args) {
		new KafkaSonsumer().consume();

	}

}
