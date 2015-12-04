package master;

import master.structures.Tweet;

public interface TweetProvider {
	
	/**
	 * Get the next tweet. This method should block until the next tweet is ready.
	 * @return Return the next tweet. NULL if there are no more tweets.
	 */
	public Tweet getNextTweet();

}
