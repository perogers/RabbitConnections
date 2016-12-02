package client;
public class FakeWorker implements Runnable{
	
	private int id;
	private String message;
	private Publisher publisher;
	
	public FakeWorker( int _id, Publisher _publisher, String _message) {
		id = _id;
		message = _message;
		publisher = _publisher;
	}
	
	@Override
	public void run() {
		try { Thread.sleep(200L); } catch (InterruptedException e) {}
		publisher.postMessage(message);
		//System.out.printf("Worker %d completed\n", id);
	}	

}
