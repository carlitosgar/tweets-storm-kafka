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
	
	private static final int SIZE = 500; //Hardcoded size for testing.
	private static final int ADVANCE = 3; //Hardcoded advance for testing.
	
	private final String topic;
	private final Properties props = new Properties();
	private static int window = 0;
	private static int lowWindowTs = -1;
	private static int topWindowTs = -1;
	private static Long currentTs = 0L;
	private static Encoder<Tweet> tweetEncoder = new TweetEncoder(null);
	private static KafkaProducer<String, byte[]> producer;
	
	public TweetKafkaProducer (String topic){
		this.topic = topic;
		InputStream in = null;
		try {
			in = new FileInputStream("src/master/producer.properties");
			this.props.load(in);
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
		Long ts = this.tweetTimeStamp(tweet.getTimestamp());
		if(ts >= currentTs){ // Discard if doesen't belong to current window.
			tweet.setTimestamp(ts);
			bytesTweet = tweetEncoder.toBytes(tweet);
			msg = new ProducerRecord<String, byte[]>(this.topic,bytesTweet);
			producer.send(msg);
			currentTs = ts;
		}
	}
	
	/**
	 * Calculate Tweet's time stamp relative to window logic.
	 * @param ts
	 * @return windowTimeStamp
	 */
	private Long tweetTimeStamp(Long ts){
		int tsInSec = (int) (ts / 1000);
		int bucket;
		if (lowWindowTs < 0){
			lowWindowTs = tsInSec;
			topWindowTs = lowWindowTs + SIZE;
		} else if (tsInSec > topWindowTs){
			lowWindowTs = topWindowTs + 1;
			topWindowTs = lowWindowTs + SIZE;
			window++;
		}
		bucket = (tsInSec - lowWindowTs) % SIZE;
		return (long) window * SIZE + bucket;
	}
	
	public void close(){
		producer.close();
	}
	
}
