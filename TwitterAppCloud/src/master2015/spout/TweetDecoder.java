package master2015.spout;

import java.io.IOException;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;
import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import master.structures.Tweet;

public class TweetDecoder implements Scheme {
	
	private static final ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public List<Object> deserialize(byte[] msg) {
		List<Object> tweets = null;
		Tweet tweet;
		try {
			tweet = objectMapper.readValue(msg, Tweet.class);
			tweets = Utils.tuple(tweet.getLanguage(),tweet.getHashtags(),tweet.getTimestamp());			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return tweets;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("language","hashtags","timestamp");
	}

}
