package master;

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;

public class TweetKafkaProducer{
	
	private final String topic;
	private final Properties props = new Properties();
	private static KafkaProducer<String, String> producer;
	
	public TweetKafkaProducer (String topic, String broker){
		this.topic = topic;
		this.props.put("bootstrap.servers", broker);
		this.props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		this.props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		this.props.put("partitioner.class", "master.LangPartitioner");
		this.props.put("request.required.acks", "1");
		producer = new KafkaProducer<String, String>(props);
	}
	
	/**
	 * Publish a message to Kafka broker (Thread safe).
	 * @param tweet
	 * @see kafka.clients.producer.KafkaProducer#send(kafka.clients.producer.ProducerRecord)
	 */
	public void publishMsg(Tweet tweet){
		ProducerRecord<String, String> msg;
		msg = new ProducerRecord<String, String>(this.topic,tweet.getLanguage(), 
					tweet.getHashtags().toString() + "," + tweet.getTimestamp());
		producer.send(msg);
	}
	
	public void close(){
		producer.close();
	}
	
}
