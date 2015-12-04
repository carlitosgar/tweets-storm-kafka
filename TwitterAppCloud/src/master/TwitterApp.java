package master;

import java.io.File;
import java.io.FileNotFoundException;

import org.apache.kafka.clients.producer.ProducerRecord;

public class TwitterApp {

	public static void main(String[] args) {
		
		if(args.length < 2) {
			System.err.println("Invalid parameters");
			return;
		}
		
		TweetProvider provider;
		
		switch(args[0]) {
		case "1": //Read from file
			File file = new File(args[1]);
			
			try {
				provider = new FileTweetProvider(file);
			} catch (FileNotFoundException e) {
				System.err.println("Can not read file.");
				return;
			}
			
			break;
			
		case "2": //Read from twitter
			provider = new TwitterTweetProvider(args[1], args[2], args[3], args[4]);
			break;
			
		default: 
			System.err.println("Invalid mode parameter");
			return;
		}
		
		Tweet tweet;
		// Hardcoded for local testing in my machine.
		TweetKafkaProducer producer = new TweetKafkaProducer("twitterStream","localhost:9092");
		
		while((tweet = provider.getNextTweet()) != null) {
			System.out.println(tweet);
			producer.publishMsg(tweet);
		}
		producer.close();
		
	}

}
