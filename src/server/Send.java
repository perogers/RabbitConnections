package server;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class Send {

  private final static String QUEUE_NAME = "hello";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    //factory.setHost("147.117.67.11");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
    
    String message = "Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!";
    for(int i =0; i < 50000; i++) {
	    channel.basicPublish("", QUEUE_NAME, MessageProperties.TEXT_PLAIN, message.getBytes("UTF-8"));
	    System.out.println(" [x] Sent '" + message + "'");
	    try { Thread.sleep(2L); } catch (InterruptedException ie) { break; }
    }
    channel.close();
    connection.close();
  }
}