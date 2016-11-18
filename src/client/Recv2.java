package client;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

public class Recv2 {

	
	 final static String QUEUE_NAME = "hello";

	  public static void main(String[] argv) throws Exception {
	    ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost("127.0.0.1");
	    factory.setAutomaticRecoveryEnabled( false );
	    factory.setConnectionTimeout(2000);
	   //factory.setHost("147.117.67.11");

	    
	    
	    for (int i=0; i <20; i++) {
	    	Receiver r = new Receiver(i, factory);
	    	new Thread(r).start();
	    }

	    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
	  }
}


class Receiver implements Runnable {
	private int id;
	private ConnectionFactory factory;
	private Connection connection;
	
	boolean stop = false;
	
	
	Receiver(int _id, ConnectionFactory _factory) {
		id = _id;
		factory = _factory;
	}

	@Override
	public void run() {
		Channel channel = null;
		try {
			
			QueueingConsumer consumer = null; 
			Delivery delivery = null;
			String message = null;
			while (!stop) {
				try {
					if( channel == null || !channel.isOpen() ) {
						channel = createChannel();
					}
					consumer = (QueueingConsumer) channel.getDefaultConsumer();
					delivery = consumer.nextDelivery(200);
					if( delivery != null ) {
						message = new String( delivery.getBody(), "UTF-8" );
						System.out.println(" [x] Received '" + message + "' by " + id);
						channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
					}
				}
				catch(Exception e) {
					e.printStackTrace();
					
				}
			}
		}catch (Exception e) {
			System.err.println(e);
		}
		finally {
			if( channel != null) {
				try {
					channel.close();
				}
				catch(Exception e) {
					System.err.println(e.getMessage());
				}
			}
		}
		
	}
	
	
	private Channel createChannel() throws IOException, TimeoutException {
		if( connection == null ) {
			connection = factory.newConnection();
		}
		else if( ! connection.isOpen() ) {
			connection.abort();
			connection = factory.newConnection();
		}
		Channel channel = connection.createChannel();
		channel.queueDeclare(Recv2.QUEUE_NAME, false, false, false, null);
		QueueingConsumer consumer = new QueueingConsumer(channel);
		channel.setDefaultConsumer(consumer);
		channel.basicConsume(Recv2.QUEUE_NAME, false, consumer);
		return channel;
	}
	
}
