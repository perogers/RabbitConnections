package rabbit_test;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.ShutdownSignalException;

public class Publisher {
	private String destinationQueue;
	private Connection connection;
	private Channel channel;
	private final ConcurrentLinkedQueue<String> messageQueue;
	private volatile boolean shutdown = false;
	
	public Publisher(String _destinationQueue, Connection _connection) {
		destinationQueue = _destinationQueue;
		connection = _connection;
		messageQueue = new ConcurrentLinkedQueue<String>();
	}
	
	void startMonitor() {
		
		if ( destinationQueue == null ) {
			System.out.println("Publisher not running");
			return;
		}
		new Thread() {
			public void run() {
				System.out.println("Publisher started for " + destinationQueue);
				try {
					fetchChannel();
					while (! shutdown ) {
						String message = messageQueue.poll();
						if( message != null) {
							if (channel == null) {
								fetchChannel();
							}
							channel.basicPublish("", destinationQueue, MessageProperties.TEXT_PLAIN, message.getBytes("UTF-8"));
							//System.out.println(" [x] Sent '" + message + "'");
						}
					}
				}
				catch (IOException e) {
					e.printStackTrace();
					return;
				}
				catch (ShutdownSignalException shutdownEx) {
					System.out.println("++++ Publisher got shutdown signal");
				}
			}
		}.start();
	}
	
	public void fetchChannel() throws IOException {
		channel = connection.createChannel();
		channel.queueDeclare(destinationQueue, false, false, false, null);
	}
	
	
	public void shutdown() {
		if ( destinationQueue == null ) {
			return;
		}
		try {
			channel.close();
		}
		catch (Exception e) {
			System.err.println("++++ Publisher - problem when closing channel:" + e.getMessage());
		}
		System.out.println("++++ Publisher got shutdown request");
		shutdown = true;
	}
	
	public void postMessage(String message) {
		if ( destinationQueue == null ) {
			return;
		}
		messageQueue.add(message);
	}

}

