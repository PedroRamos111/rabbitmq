 class Broker implements Runnable{
    public static void main(String[] args) {
        
        Thread t1 = new Thread(new Broker());

		t1.start();

		
    }

	@Override
	public void run() {
		System.out.println(Thread.currentThread().getState());

		System.out.println("Programa em execucao...");
        
	}
}
