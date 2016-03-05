package com.systems.s290.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.util.consistenthash.ConsistentHash;

public class ConsistentHashSplit implements DataSplit {

	static final Logger LOG = LoggerFactory.getLogger(DataSplit.class);
	private ConsistentHash<String> consistentHash;
	private SystemDetails systemDetails;
	
	public ConsistentHashSplit(SystemDetails systemDetails) {
		consistentHash = new ConsistentHash<String>(systemDetails.getServerCount(), systemDetails.getConnectionStrings());
		this.systemDetails = systemDetails;
	}
	
	@Override
	public int getServerIndex(TwitterStatus status) {
		Long primaryKeyValue = status.getUserId();
		String bin = consistentHash.getBinFor(primaryKeyValue);
		return systemDetails.getTargetConnectionStrings().indexOf(bin);
	}



	@Override
	public String getTargetTableName() {
		return "TweetsC";
	}

	

}
