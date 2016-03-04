package com.systems.s290.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RequestGenerator {
	
	private List<Long> userIds = new ArrayList<>();
	private Random randomizer = new Random();
	ExecutorService newCachedThreadPool = Executors.newCachedThreadPool();
	 
	
	private void readUserIds() throws IOException{
		
		try(BufferedReader userIdReader = new BufferedReader(new FileReader(new File("resources/userIds")))){
			String userId = null;
			while((userId = userIdReader.readLine()) != null){
				userIds.add(Long.parseLong(userId));
			}
		}
		
		
	}
	
	private Long getRandomUserId(){
		return userIds.get(randomizer.nextInt(userIds.size()));
	}

	
	public void fireRandomRequest(){
		
		
	}
}
