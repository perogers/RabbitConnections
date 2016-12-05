package rabbit_test;

public class ManagerRunner {


	public static void main(String[] argv) throws Exception {
		if (argv.length < 1) {
			System.err.println("Manager Runner <Source Queue Name> <Destination Queue Name - optional>");
			System.exit(1);
		}
		String srcQ = argv[0];
		String destQ = null;
		if (argv.length >1 ) {
			destQ = argv[1];
		}
		System.out.printf("Starting for Source Q: '%s'  Destination Q: '%s'\n", srcQ, destQ);
		TestAmqpManager sdk = new TestAmqpManager(srcQ, destQ, "localhost");
		sdk.start();
	}

}
