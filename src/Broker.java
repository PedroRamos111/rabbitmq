import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.nio.charset.StandardCharsets;

class Broker implements Runnable {

	private static final String EXCHANGE_NAME = "topic_logs";
	private static Scanner entrada = new Scanner(System.in);

	public static void main(String[] args) throws IOException, TimeoutException {
		System.out.println("Qual o nome da sua corretora?");
		String corretora = entrada.nextLine();
		menu(corretora);
	
	}

	public static void menu(String corretora) throws IOException, TimeoutException {
		int op;
		do {
			System.out.println("Selecione uma opção:");
			System.out.println("1 - Comprar ações");
			System.out.println("2 - Vender ações");
			System.out.println("0 - Sair");

			op = entrada.nextInt();
			switch (op) {
				case 1:
					entrada.nextLine();
					System.out.println("Qual açao você quer comprar?(Sigla)");
					String acao = entrada.nextLine();
					System.out.println("Quantos desse ativo você quer comprar?");
					int quant = entrada.nextInt();
					System.out.println("Qual valor você pretende pagar neste ativo?");
					double valor = entrada.nextDouble();
					compra(corretora, acao, quant, valor);
					break;

				case 2:
					entrada.nextLine();
					System.out.println("Qual açao você quer vender?(Sigla)");
					acao = entrada.nextLine();
					System.out.println("Quantos desse ativo você quer vender?");
					quant = entrada.nextInt();
					System.out.println("Qual valor você pretende vender este ativo por?");
					valor = entrada.nextDouble();
					venda(corretora, acao, op, op);
					break;
				case 0:
				entrada.nextLine();
				main(null);
					break;
				default:
					entrada.nextLine();
					System.out.println("Esse não é um valor valido");
					break;
			}
		} while (op != 0);
	}

	@Override
	public void run() {
		System.out.println(Thread.currentThread().getState());

		System.out.println("Programa em execucao...");

	}

	public static void compra(String corretora, String ativo, int quant, double val)
			throws IOException, TimeoutException {
		String topic = "compra." + ativo;
		String message = corretora + ";" + quant + ";" + val;
		enviaPedido(topic, message);
	}

	public static void venda(String corretora, String ativo, int quant, double val)
			throws IOException, TimeoutException {
		String topic = "venda." + ativo;
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
