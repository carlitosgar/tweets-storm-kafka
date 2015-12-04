package master;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerRecord;

import master.structures.Tweet;

import org.apache.kafka.clients.producer.KafkaProducer;

public class TweetKafkaProducer{
	
	private final String topic;
	private final Properties props = new Properties();
	private static KafkaProducer<String, String> producer;
	
	public TweetKafkaProducer (String topic){
		this.topic = topic;
		InputStream in = null;
		try {
			in = new FileInputStream("src/master/producer.properties");
			this.props.load(in);
		} catch(IOException e){
			System.out.println(e.toString());
		}
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
