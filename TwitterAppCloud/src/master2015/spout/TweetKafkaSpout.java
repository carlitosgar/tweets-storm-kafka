package master2015.spout;

import java.util.UUID;
import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

public class TweetKafkaSpout{
	
	/**
	 * Kafka-Spout. Storm's spout integration for Kafka consuming.
	 * @see storm.kafka.KafkaSpout
	 */
	private KafkaSpout spout; 

	public TweetKafkaSpout (String topic, String zkConn){
		BrokerHosts hosts = new ZkHosts(zkConn);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new TweetDecoder());
		spoutConfig.startOffsetTime= kafka.api.OffsetRequest.EarliestTime();		
		this.spout = new KafkaSpout(spoutConfig);		
	}
	
	public KafkaSpout getSpout(){
		return this.spout;
	}

}
