public class Broker {
    public static void main(String[] args) {
        
        Thread t1 = new Thread(new Bolsa());

		
		System.out.println(t1.toString()); 

		
		System.out.println(t1.getState());

		t1.start();

		System.out.println(t1.getState());
    }
}
