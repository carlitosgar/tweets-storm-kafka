package master;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerRecord;
import kafka.serializer.Encoder;
import master.structures.Tweet;
import org.apache.kafka.clients.producer.KafkaProducer;

public class TweetKafkaProducer{
	
	private final String topic;
	private final Properties props = new Properties();
	private static Encoder<Tweet> tweetEncoder = new TweetEncoder(null);
	private static KafkaProducer<String, byte[]> producer;
	
	public TweetKafkaProducer (String topic, String kafkaUrl){
		this.topic = topic;
		try {
			
			//Load configuration from file
			InputStream in = new FileInputStream("src/master/producer.properties");
			this.props.load(in);
			
			//Kafka url
			this.props.put("bootstrap.servers", kafkaUrl);
			this.props.put("partitioner.class", "master.LangPartitioner");
			
		} catch(IOException e){
			System.out.println(e.toString());
		}
		
		producer = new KafkaProducer<String, byte[]>(props);
	}
	
	/**
	 * Publish a message to Kafka broker (Thread safe).
	 * @param tweet
	 * @see kafka.clients.producer.KafkaProducer#send(kafka.clients.producer.ProducerRecord)
	 */
	public void publishMsg(Tweet tweet){
		ProducerRecord<String, byte[]> msg;
		byte[] bytesTweet;
		bytesTweet = tweetEncoder.toBytes(tweet);
		msg = new ProducerRecord<String, byte[]>(this.topic, tweet.getLanguage(), bytesTweet);
		producer.send(msg);
	}
	
	public void close(){
		producer.close();
	}

	public void sendBlankTuple() {
		Tweet blankTweet = new Tweet("0", null, null);
		byte[] bytesTweet = tweetEncoder.toBytes(blankTweet);
		ProducerRecord<String, byte[]> msg = new ProducerRecord<String, byte[]>(this.topic, "",bytesTweet);
		producer.send(msg);
	}
	
}
