package master;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class JsonTweetProvider implements TweetProvider {
	
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
			
			return new Tweet(createdAt, hashtags);
		}
			
		return null;
		
	}

}
