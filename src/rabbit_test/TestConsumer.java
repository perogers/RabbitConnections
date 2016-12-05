package rabbit_test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

public class TestConsumer extends DefaultConsumer {
	
	private int id;
	private ExecutorService executor;
	private Publisher publisher;
	
	public TestConsumer(Channel _channel, int _id, ExecutorService _executor, Publisher _publisher) {
		super(_channel);
		id = _id;
		executor = _executor;
		publisher = _publisher;
	}
	
	@Override
    public void handleDelivery(String consumerTag,
                               Envelope envelope,
                               AMQP.BasicProperties properties,
                               byte[] body)
        throws IOException
    {
       // String routingKey = envelope.getRoutingKey();
       // String contentType = properties.getContentType();
        long deliveryTag = envelope.getDeliveryTag();
        String message = new String(body, "UTF-8");
        System.out.printf(" %d Received '%s'\n", id,  message);
        Channel channel = getChannel();
		if( !executor.isShutdown() ) {
			executor.submit(new FakeWorker(id, publisher, message));
			channel.basicAck(deliveryTag, false);
		}
	}


	@Override
	public void handleConsumeOk(String consumerTag) {
		super.handleConsumeOk(consumerTag);
		System.out.printf("++++ handleConsumeOk for consumer %d\n", id);
	}


	@Override
	public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
		super.handleShutdownSignal(consumerTag, sig);
		System.out.printf("++++ handleShutdownSignal for consumer %d\n", id);
		executor.shutdown();
		publisher.shutdown();
	}
	

}
