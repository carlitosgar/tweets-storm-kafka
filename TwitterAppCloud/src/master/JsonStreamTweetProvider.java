package master;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class JsonStreamTweetProvider implements TweetProvider {
	
	protected abstract BufferedReader getReader();


	/**
	 * 
	 * @param jsonStr
	 * @return Null means it was not a valid tweet.
	 * @throws IOException
	 */
	protected Tweet getTweetFromJson(String jsonStr) throws IOException {
		
		ObjectMapper mapper = new ObjectMapper();
		JsonNode json = mapper.readTree(jsonStr);
		
		if(json.has("created_at")) {
			
			String createdAt = json.get("created_at").asText();
			
			List<String> hashtags = new LinkedList<String>();
			for(JsonNode hashtag : json.get("entities").get("hashtags")) {
				hashtags.add(hashtag.get("text").asText());
			}
			
			String language = json.get("lang").asText();
			
			return new Tweet(createdAt, hashtags, language);
		}
			
		return null;
		
	}
	
	public Tweet getNextTweet() {
		
		try {
			String jsonLine;
			Tweet tweet;
			BufferedReader reader = this.getReader();
			
			while((jsonLine = reader.readLine()) != null) {
				tweet = this.getTweetFromJson(jsonLine);
				
				if(tweet != null) {
					return tweet;
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();			
		}
		
		return null;
	}

}
