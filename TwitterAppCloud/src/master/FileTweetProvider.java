package master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class FileTweetProvider extends JsonStreamTweetProvider {
	
	private BufferedReader reader;

	public FileTweetProvider(File file) throws FileNotFoundException {
		super();
		this.reader = new BufferedReader(new FileReader(file));
	}

	@Override
	protected BufferedReader getReader() {
		return this.reader;
	}


}
