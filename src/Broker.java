import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.nio.charset.StandardCharsets;

class Broker implements Runnable{

	private static final String EXCHANGE_NAME = "topic_logs";
    public static void main(String[] args) {
        
        Thread t1 = new Thread(new Broker());

		t1.start();
		try {
			venda("BRK1","ABEV3", 10, 10);
			//compra("BRK2","ABEV3", 10, 10.0);
			
		} catch (IOException | TimeoutException e) {
			e.printStackTrace();
		}
    }

	@Override
	public void run() {
		System.out.println(Thread.currentThread().getState());

		System.out.println("Programa em execucao...");
        
	}

	public static void compra(String corretora, String ativo, int quant, double val) throws IOException, TimeoutException {
		String topic = "compra."+ativo;
		String message = corretora + ";" + quant + ";" + val;
		enviaPedido(topic, message);
	}
	
	public static void venda(String corretora, String ativo, int quant, double val) throws IOException, TimeoutException {
		String topic = "venda."+ativo;
		String message = corretora + ";" + quant + ";" + val;
		enviaPedido(topic, message);
	}
	
	public static void enviaPedido(String topic, String message) throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("gull.rmq.cloudamqp.com");
		factory.setUsername("zwzsdwdx");
		factory.setPassword("dIPnl1KCfla3vDb6FzjDOLh30BP-mrtu");
		factory.setVirtualHost("zwzsdwdx");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
	
		channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
	
		channel.basicPublish(EXCHANGE_NAME, topic, null, message.getBytes("UTF-8"));
		System.out.println(" [x] Sent '" + topic + "':'" + message + "'");
	
		channel.close();
		connection.close();
	}
	


}
