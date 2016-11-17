package client;

import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

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
			Consumer consumer = new DefaultConsumer(channel) {
			      @Override
			      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
			          throws IOException {
			        String message = new String(body, "UTF-8");
			        System.out.println(" [x] Received '" + message + "' by " + id);
			      }
			};
			while (!stop) {
				channel.basicConsume(Recv2.QUEUE_NAME, true, consumer);
			}
		}catch (Exception e) {
			System.err.println(e);
		}
		
	}
	
}
