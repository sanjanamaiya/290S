package com.systems.s290.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestGenerator {
	
	private List<Long> userIds = new ArrayList<>();
	private Random randomizer = new Random();
	ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(10);
	static final Logger LOG = LoggerFactory.getLogger(RequestGenerator.class);
	static final Logger statichashLogger = LoggerFactory.getLogger("static"); 
	static final Logger consistenthashLogger = LoggerFactory.getLogger("consistent"); 
	
	public RequestGenerator(){
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable(){

			@Override
			public void run() {
				newFixedThreadPool.shutdown();
				
			}
			
		}));
	}
	
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

	
	public void fireRandomRequest() throws IOException, SQLException, InterruptedException{
		LOG.info("Starting request firing");
		readUserIds();
		RequestHandler handler = new RequestHandler();
		long startTime = System.nanoTime();
		long endTime = startTime + TimeUnit.NANOSECONDS.convert(15L, TimeUnit.MINUTES);
		long addTime = startTime + TimeUnit.NANOSECONDS.convert(1L, TimeUnit.MINUTES);
		long removeTime = startTime + TimeUnit.NANOSECONDS.convert(5L, TimeUnit.MINUTES);
		boolean serverAdded = false;
		boolean serverRemoved = false;
		while(System.nanoTime() < endTime){
			//Run for some time
			Long randomUserId = getRandomUserId();
			//LOG.info("Requesting for user:"+randomUserId);
			newFixedThreadPool.submit(new RequestUser(randomUserId,handler));
			
			//Initiate an addition
			if(System.nanoTime() > addTime && !serverAdded){
				LOG.info("Adding server");
				consistenthashLogger.info("Adding server");
				statichashLogger.info("Adding server");
				handler.addServer();
				serverAdded = true;
				LOG.info("Adding server completed");
				consistenthashLogger.info("Adding server completed");
				statichashLogger.info("Adding server completed");
			}
			//Initiate a removal
			else if((System.nanoTime()) > removeTime && !serverRemoved){
				LOG.info("Removing server");
				consistenthashLogger.info("Removing server");
				statichashLogger.info("Removing server");
				handler.removeServer();
				serverRemoved = true;
				LOG.info("Removing server completed");
				consistenthashLogger.info("Removing server completed");
				statichashLogger.info("Removing server completed");
			}
			else{
				Thread.sleep(1);
			}
		}
		
		
		
	}
	
	public static void main(String[] args) throws IOException, SQLException, InterruptedException {
		new RequestGenerator().fireRandomRequest();
	}
}

class RequestUser implements Runnable{
 
	private long userId;
	private RequestHandler handler;
	
	
	public RequestUser(long userId, RequestHandler handler){
		this.userId = userId;
		this.handler = handler;
	}
	
	@Override
	public void run() {
		long startTime = System.nanoTime();
		handler.getTweetsFromUser(userId+"",RequestHandler.CONSISTENT );
		RequestGenerator.consistenthashLogger.info((System.nanoTime() - startTime)+"");
		//startTime = System.nanoTime();
		//handler.getTweetsFromUser(userId+"",RequestHandler.STATIC );
		//statichashLogger.info((System.nanoTime() - startTime)+"");
	}
	
}