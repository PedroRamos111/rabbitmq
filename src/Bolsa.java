import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.StringTokenizer;
import java.util.concurrent.TimeoutException;

public class Bolsa {

    private static final String EXCHANGE_NAME = "topic_logs";
    private static final String arqLivro = "POO_Livro.csv";
    private static List<String> dadosList = new ArrayList<String>();

    public static void main(String[] args) {

        Thread threadRecv = new Thread(new Runnable() {
            public void run() {

                try {
                    recebePedido();
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        });
        Thread threadSend = new Thread(new Runnable() {
            public void run() {

                System.out.println("Executando a segunda função");
            }
        });

        threadRecv.start();
        // threadSend.start();
    }

    public static void recebePedido() throws IOException, TimeoutException {

        String[] listTopics = new String[2];
        listTopics[0] = "compra";
        listTopics[1] = "venda";
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("gull.rmq.cloudamqp.com");
        factory.setUsername("zwzsdwdx");
        factory.setPassword("dIPnl1KCfla3vDb6FzjDOLh30BP-mrtu");
        factory.setVirtualHost("zwzsdwdx");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        String queueName = channel.queueDeclare().getQueue();

        if (listTopics.length < 1) {
            System.err.println("Usage: ReceiveLogsTopic [binding_key]...");
            System.exit(1);
        }

        channel.queueBind(queueName, EXCHANGE_NAME, "compra.#");
        channel.queueBind(queueName, EXCHANGE_NAME, "venda.#");

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {

            String message = new String(delivery.getBody(), "UTF-8");
            String routingKey = delivery.getEnvelope().getRoutingKey();

            System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
            System.out.println(routingKey);
            System.out.println(message);
            Thread threadLivro = new Thread(new Runnable() {
                public void run() {

                    checkMatch(message, routingKey);
                    registraLivro(routingKey, message);
                }
            });
            threadLivro.start();

        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }

    public static void registraLivro(String key, String msg) {

        try {
            FileWriter arquivo = new FileWriter(arqLivro, false);
            try {
                arquivo.write(key + ";" + msg + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
            arquivo.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void getDadosLivro() {

        try {
            BufferedReader reader = new BufferedReader(new FileReader(arqLivro));
            String linha;

            while ((linha = reader.readLine()) != null) {
                StringTokenizer str = new StringTokenizer(linha, "\n");
                String dadosL = str.nextToken();
                String []dados = dadosL.split(";");
                String key = dados[0] + "." + dados[1];
                String message = dados[2] + ";" + dados[3] + ";" + dados[4];
                checkMatch(message, key);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void checkMatch(String message, String key) {

        String[] dadosK = key.split(".");
        String[] dadosM = message.split(";");
        Boolean achou = false;
        for (int i = 0; i < dadosList.size(); i++) {
            String[] aux = dadosList.get(i).split(";");
            if (dadosK[0] != aux[0]) {
                if (dadosK[1] == aux[1]) {

                    if (dadosK[0] == "compra") {
                        if (Integer.parseInt(dadosM[0]) < Integer.parseInt(aux[2])) {
                            if (Double.parseDouble(dadosM[1]) >= Double.parseDouble(aux[3])) {
                                String temp = dadosList.remove(i);
                                String[] aux2 = temp.split(";");
                                aux2[2] = Integer.toString(Integer.parseInt(aux2[2]) - Integer.parseInt(dadosM[0]));
                                temp = aux2[0] + ";" + aux2[1] + ";" + aux2[2] + ";" + aux2[3] + ";" + aux2[4];
                                dadosList.add(temp);
                                achou = true;
                                //chamar metodo do componente transações aqui
                            }
                        } else if (Integer.parseInt(dadosM[0]) == Integer.parseInt(aux[2])) {
                            dadosList.remove(i);
                            //chamar metodo do componente transações aqui
                        }
                    } else {
                        if (Integer.parseInt(dadosM[0]) > Integer.parseInt(aux[2])) {
                            if (Double.parseDouble(dadosM[1]) <= Double.parseDouble(aux[3])) {
                                String temp = dadosList.remove(i);
                                String[] aux2 = temp.split(";");
                                dadosM[0] = Integer
                                        .toString(Integer.parseInt(dadosM[2]) - Integer.parseInt(aux2[0]));
                                temp = dadosK[0] + ";" + dadosK[1] + ";" + dadosM[0] + ";" + dadosM[1] + ";"
                                        + dadosM[2];
                                dadosList.add(temp);
                                achou = true;
                                //chamar metodo do componente transações aqui
                            }
                        } else if (Integer.parseInt(dadosM[0]) == Integer.parseInt(aux[2])) {
                            dadosList.remove(i);
                            //chamar metodo do componente transações aqui
                        }

                    }

                }
            }
        }

        if (achou == false) {

            dadosList.add(key + ";" + dadosM[0] + ";" + dadosM[1] + ";" + dadosM[2]);
        }
    }




}
