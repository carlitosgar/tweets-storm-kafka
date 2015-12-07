package master;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import master.structures.Tweet;

public class TweetEncoder implements Encoder<Tweet>{
	
	private static final ObjectMapper objectMapper = new ObjectMapper();

	public TweetEncoder(VerifiableProperties verifiableProperties) {
        /* This constructor must be present for successful compile. */
    }
	
	@Override
	public byte[] toBytes(Tweet tweet) {
		try {
			return objectMapper.writeValueAsString(tweet).getBytes();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return "".getBytes();
	}

}
