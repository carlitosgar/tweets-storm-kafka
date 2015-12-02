package master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.scribe.builder.ServiceBuilder;
import org.scribe.builder.api.TwitterApi;
import org.scribe.model.OAuthRequest;
import org.scribe.model.Response;
import org.scribe.model.Token;
import org.scribe.model.Verb;
import org.scribe.oauth.OAuthService;


public class TwitterJsonTweetProvider extends JsonTweetProvider {
	
	private static final String STREAM_URL = "https://stream.twitter.com/1.1/statuses/sample.json";
	private BufferedReader reader;
	
	public TwitterJsonTweetProvider(String apiKey, String apiSecret, String token, String secret) {
		
		OAuthService service = new ServiceBuilder()
                .provider(TwitterApi.class)
                .apiKey(apiKey)
                .apiSecret(apiSecret)
                .build();
		
		Token accessToken = new Token(token, secret);
		
		OAuthRequest request = new OAuthRequest(Verb.GET, STREAM_URL);
        service.signRequest(accessToken, request);
        Response response = request.send();
        
        this.reader = new BufferedReader(new InputStreamReader(response.getStream()));

		
	}

	@Override
	public Tweet getNextTweet() {
		
		try {
			String jsonLine;
			Tweet tweet;
			
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
