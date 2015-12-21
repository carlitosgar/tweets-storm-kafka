package master;

import java.io.File;
import java.io.FileNotFoundException;

import org.apache.kafka.clients.producer.ProducerRecord;

import master.structures.Tweet;

public class TwitterApp {

	public static void main(String[] args) {
		
		if(args.length < 2) {
			System.err.println("Invalid parameters");
			return;
		}
		
		TweetProvider provider;
		String kafkaUrl;
		
		switch(args[0]) {
		case "1": //Read from file
			File file = new File(args[2]);
			
			try {
				provider = new FileTweetProvider(file);
			} catch (FileNotFoundException e) {
				System.err.println("Can not read file " + file.getAbsolutePath());
				return;
			}
			
			kafkaUrl = args[5];
			
			break;
			
		case "2": //Read from twitter
			provider = new TwitterTweetProvider(args[1], args[2], args[3], args[4]);
			kafkaUrl = args[5];
			break;
			
		default: 
			System.err.println("Invalid mode parameter");
			return;
		}
		
		Tweet tweet;
		TweetKafkaProducer producer = new TweetKafkaProducer("twitterStream", kafkaUrl);
		
		while((tweet = provider.getNextTweet()) != null) {
			System.out.println(tweet);
			producer.publishMsg(tweet);
		}
		
		//Send the final blank tuple
		producer.sendBlankTuple();
		
		//Close kafka producer
		producer.close();
		
	}

}
