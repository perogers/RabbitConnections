package rabbit_test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class TestAmqpManager extends Thread {
	
	private String sourceQueueName;
	private String destinationQueueName;
	private ExecutorService executor;
	
	TestAmqpManager(String _sourceQueueName, String _destinationQueueName) {
		sourceQueueName = _sourceQueueName;
		destinationQueueName = _destinationQueueName;
	}
	

	@Override
	public void run() {
		
		try {
			executor = Executors.newFixedThreadPool(100);
		}
		catch (IllegalArgumentException e) {
			e.printStackTrace();
			return;
		}
		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("127.0.0.1");
			factory.setAutomaticRecoveryEnabled( false );
			factory.setConnectionTimeout(500);
			factory.setShutdownTimeout(500);
			
			final Connection connection1 = factory.newConnection();
			final Publisher publisher = new Publisher(destinationQueueName, connection1);
			
			List<Channel> channels = buildSubscriberChannels(connection1, publisher);
			publisher.startMonitor();
			
			
			for (final Channel c : channels) {
				new Thread() {
					public void run() {
						try {
							c.basicConsume(sourceQueueName, false, c.getDefaultConsumer());
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}.start();
			}
			
			Runtime.getRuntime().addShutdownHook( new Thread() {
				@Override
				public void run() {
					System.out.println("****** Shutdown ******");
					
					
					try {
						if( connection1.isOpen())
							connection1.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					executor.shutdownNow();
					publisher.shutdown();
				}
			});
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private List<Channel> buildSubscriberChannels(Connection connection, Publisher publisher) throws IOException {
		List<Channel> channels = new ArrayList<Channel>();
		for (int ccnt = 0; ccnt < 1; ccnt++) {
			final Channel channel = connection.createChannel();
			channel.queueDeclare(sourceQueueName, false, false, false, null);
			channel.basicQos(10, true);
			channel.setDefaultConsumer(new TestConsumer(channel, ccnt, executor, publisher));
			channels.add(channel);
		}
		return channels;
	}

}
