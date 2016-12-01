package client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

public class RecvDefaultConsumer {

	
		final static String QUEUE_NAME = "hello";

		public static void main(String[] argv) throws Exception {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("127.0.0.1");
			factory.setAutomaticRecoveryEnabled( false );
			factory.setConnectionTimeout(2000);
			
			Connection connection = factory.newConnection();
			List<Channel> channels = new ArrayList<Channel>();
			for (int ccnt = 0; ccnt < 40; ccnt++) {
				final Channel channel = connection.createChannel();
				channel.queueDeclare(QUEUE_NAME, false, false, false, null);
				channel.setDefaultConsumer(new TestConsumer(channel, ccnt));
				channels.add(channel);
			}
			for (final Channel c : channels) {
				new Thread() {
					public void run() {
						try {
							c.basicConsume(QUEUE_NAME, false, c.getDefaultConsumer());
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}.start();
			}
		}
}

class TestConsumer extends DefaultConsumer {
	private int id;
	public TestConsumer(Channel _channel, int _id) {
		super(_channel);
		id = _id;
	}
	
	@Override
    public void handleDelivery(String consumerTag,
                               Envelope envelope,
                               AMQP.BasicProperties properties,
                               byte[] body)
        throws IOException
    {
        String routingKey = envelope.getRoutingKey();
        String contentType = properties.getContentType();
        long deliveryTag = envelope.getDeliveryTag();
        String message = new String(body, "UTF-8");
        System.out.printf(" %d Received '%s'\n", id,  message);
        getChannel().basicAck(deliveryTag, false);
        doWork();
    }
	
	
	private void doWork() {
		try { Thread.sleep(200L); } catch (InterruptedException e) {}
	}

	@Override
	public void handleCancel(String consumerTag) throws IOException {
		super.handleCancel(consumerTag);
		System.out.println("++++ handleCancel");
	}

	@Override
	public void handleCancelOk(String consumerTag) {
		super.handleCancelOk(consumerTag);
		System.out.println("++++ handleCancelOk");
	}

	@Override
	public void handleConsumeOk(String consumerTag) {
		super.handleConsumeOk(consumerTag);
		System.out.println("++++ handleConsumeOk");
	}

	@Override
	public void handleRecoverOk(String consumerTag) {
		super.handleRecoverOk(consumerTag);
		System.out.println("++++ handleRecoverOk");
	}

	@Override
	public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
		super.handleShutdownSignal(consumerTag, sig);
		System.out.println("++++ handleShutdownSignal");
	}
	
	
}