package com.systems.s290.data;

public class TwitterStatus 
{	
	private long twitterStatusId;
	private String userScreenName;
	private String text;
	private String userMentions;
	private String hashTags;
	private long userId;
	
	public long getUserId() {
		return userId;
	}
	public void setUserId(long userId) {
		this.userId = userId;
	}
	public long getTwitterStatusId() {
		return twitterStatusId;
	}
	public void setTwitterStatusId(long twitterStatusId) {
		this.twitterStatusId = twitterStatusId;
	}
	public String getUserScreenName() {
		return userScreenName;
	}
	public void setUserScreenName(String userScreenName) {
		this.userScreenName = userScreenName;
	}
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	public String getUserMentions() {
		return userMentions;
	}
	public void setUserMentions(String userMentions) {
		this.userMentions = userMentions;
	}
	public String getHashTags() {
		return hashTags;
	}
	public void setHashTags(String hashTags) {
		this.hashTags = hashTags;
	}
}