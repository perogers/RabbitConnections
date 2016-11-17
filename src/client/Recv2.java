package client;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

public class Recv2 {

	
	 final static String QUEUE_NAME = "hello";

	  public static void main(String[] argv) throws Exception {
	    ConnectionFactory factory = new ConnectionFactory();
	    //factory.setHost("127.0.0.1");
	    factory.setAutomaticRecoveryEnabled( true );
	    factory.setConnectionTimeout(2000);
	    factory.setHost("147.117.67.11");

	    Connection connection = factory.newConnection();
	    
	    for (int i=0; i <20; i++) {
	    	Receiver r = new Receiver(i, connection.createChannel());
	    	new Thread(r).start();
	    }

	    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
	  }
}


class Receiver implements Runnable {
	private int id;
	private Channel channel;
	boolean stop = false;
	
	
	Receiver(int _id, Channel _channel) {
		id = _id;
		channel = _channel;
	}

	@Override
	public void run() {
		try {
			channel.queueDeclare(Recv2.QUEUE_NAME, false, false, false, null);
			QueueingConsumer consumer = null; 
			Delivery delivery = null;
			String message = null;
			consumer = new QueueingConsumer(channel);
			channel.setDefaultConsumer(consumer);
			channel.basicConsume(Recv2.QUEUE_NAME, false, consumer);
			while (!stop) {
				try {
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
		
	}
	
}
