package com.systems.s290.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.util.consistenthash.HashFunction;
import com.cloudera.util.consistenthash.MD5HashFunction;

public class StaticHashStrategy implements HashingStrategy {

	static final Logger LOG = LoggerFactory.getLogger(HashingStrategy.class);
	private HashFunction hashFunction;
	private SystemDetails systemDetails;
	
	public StaticHashStrategy(SystemDetails systemDetails) {
		this.hashFunction = new MD5HashFunction();
		this.systemDetails = systemDetails;
	}

	@Override
	public int getServerIndex(TwitterStatus status) {
		return getHash(status.getUserId(), systemDetails.getServerCount());
	}

	@Override
	public String getTargetTableName() {
		return "TweetsS";
	}

	public int getHash(long userId, int count) {
		return hashFunction.hash(userId) % count;
	}

}
