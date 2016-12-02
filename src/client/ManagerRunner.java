package client;

public class ManagerRunner {

	final static String QUEUE_NAME = "hello";

	public static void main(String[] argv) throws Exception {
		TestAmqpManager sdk1 = new TestAmqpManager(QUEUE_NAME, QUEUE_NAME + "-bak");
		TestAmqpManager sdk2 = new TestAmqpManager(QUEUE_NAME + "-bak", null);
		sdk1.start();
		sdk2.start();
		
	}

}
