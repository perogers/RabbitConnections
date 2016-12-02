package client;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

public class Publisher {
	private String destinationQueue;
	private Connection connection;
	private Channel channel;
	private final ConcurrentLinkedQueue<String> messageQueue;
	private boolean shutdown = false;
	
	public Publisher(String _destinationQueue, Connection _connection) {
		destinationQueue = _destinationQueue;
		connection = _connection;
		messageQueue = new ConcurrentLinkedQueue<String>();
	}
	
	void startMonitor() {
		
		if ( destinationQueue == null ) {
			System.out.println("Monitor not running");
			return;
		}
		new Thread() {
			public void run() {
				System.out.println("Monitor started for " + destinationQueue);
				try {
					channel = connection.createChannel();
					channel.queueDeclare(destinationQueue, false, false, false, null);
					while (! shutdown ) {
						String message = messageQueue.poll();
						if( message != null) {
							
							if (channel == null) {
								channel = connection.createChannel();
								channel.queueDeclare(destinationQueue, false, false, false, null);
							}
							
							channel.basicPublish("", destinationQueue, MessageProperties.TEXT_PLAIN, message.getBytes("UTF-8"));
							System.out.println(" [x] Sent '" + message + "'");
						}
					}
				}
				catch (IOException e) {
					e.printStackTrace();
					return;
				}
			}
		}.start();
	}
	
	public void shutdown() {
		shutdown = true;
	}
	
	public void postMessage(String message) {
		if ( destinationQueue == null ) {
			return;
		}
		messageQueue.add(message);
	}

}

