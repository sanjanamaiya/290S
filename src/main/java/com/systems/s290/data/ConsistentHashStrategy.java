package com.systems.s290.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.util.consistenthash.ConsistentHash;

public class ConsistentHashStrategy implements HashingStrategy {

	static final Logger LOG = LoggerFactory.getLogger(HashingStrategy.class);
	private ConsistentHash<String> consistentHash;
	private SystemDetails systemDetails;
	
	public ConsistentHashStrategy(SystemDetails systemDetails) {
		consistentHash = new ConsistentHash<String>(systemDetails.getServerCount(), systemDetails.getTargetConnectionStrings());
		this.systemDetails = systemDetails;
	}
	
	@Override
	public int getServerIndex(TwitterStatus status) {
		Long primaryKeyValue = status.getUserId();
		return getHash(primaryKeyValue);
	}

	public int getHash(Long primaryKeyValue) {
		String bin = consistentHash.getBinFor(primaryKeyValue);
		return systemDetails.getTargetConnectionStrings().indexOf(bin);
	}

	public ConsistentHash<String> getConsistentHash()
	{
		return consistentHash;
	}

	@Override
	public String getTargetTableName() {
		return "TweetsC";
	}

	@Override
	public String getDistributedDirTableName() {
		return "";
	}

	

}
