class Bolsa implements Runnable {

	@Override
	public void run() {
		System.out.println(Thread.currentThread().getState());

		System.out.println("Programa em execucao...");
        
	}
	
}