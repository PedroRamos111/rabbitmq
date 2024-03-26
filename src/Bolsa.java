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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeoutException;

public class Bolsa {

    private static final String EXCHANGE_NAME = "topic_logs";
    private static final String arqLivro = "POO_Livro.csv";
    private static final String arqTransacoes = "POO_Transacao.csv";
    private static List<String> dadosList = new ArrayList<String>();
    private static List<String> transacoesList = new ArrayList<String>();
    private static DateTimeFormatter formatador = DateTimeFormatter.ofPattern("dd/MM/yyyy-HH:mm:ss");

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
        // n sei se vc n terminou ainda mas isso n funfa n vai ser complicado fazer ele funfar
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
                String[] dados = dadosL.split(";");
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

        String[] dadosK = key.split("\\.");
        String[] dadosM = message.split(";");
        Boolean achou = false;
        for (int i = 0; i < dadosList.size(); i++) {
            String[] aux = dadosList.get(i).split(";");
            if (!dadosK[0].equals(aux[0].split("\\.")[0])) {
                //System.out.println("0");
                if (dadosK[1].equals(aux[0].split("\\.")[1])) {
                    //System.out.println("1");
                    if (dadosK[0].equals("compra")) {
                        //System.out.println("2");
                        if (Integer.parseInt(dadosM[1]) < Integer.parseInt(aux[2])) {
                            //System.out.println("3");
                            if (Double.parseDouble(dadosM[2]) >= Double.parseDouble(aux[3])) {
                                //System.out.println("4");
                                String temp = dadosList.remove(i);
                                String[] aux2 = temp.split(";");
                                aux2[2] = Integer.toString(Integer.parseInt(aux2[2]) - Integer.parseInt(dadosM[1]));
                                temp = aux2[0] + ";" + aux2[1] + ";" + aux2[2] + ";" + aux2[3];
                                dadosList.add(temp);
                                achou = true;
                                registraTransacao(dadosK[1], Integer.parseInt(dadosM[1]), Double.parseDouble(dadosM[2]),
                                        dadosM[0], aux[1]);
                            }
                        } else if (Integer.parseInt(dadosM[1]) == Integer.parseInt(aux[2])) {
                            //colocar verificação de preço
                            //System.out.println("5");
                            dadosList.remove(i);
                            registraTransacao(dadosK[1], Integer.parseInt(dadosM[1]), Double.parseDouble(dadosM[2]),
                                    dadosM[0], aux[1]);
                                    
                        }
                    } else {
                        if (Integer.parseInt(dadosM[1]) > Integer.parseInt(aux[2])) {
                            //System.out.println("6");
                            if (Double.parseDouble(dadosM[2]) <= Double.parseDouble(aux[3])) {
                                //System.out.println("7");
                                String temp = dadosList.remove(i);
                                String[] aux2 = temp.split(";");
                                dadosM[1] = Integer
                                        .toString(Integer.parseInt(dadosM[1]) - Integer.parseInt(aux2[2]));
                                temp = dadosK[0] + "." + dadosK[1] + ";" + dadosM[0] + ";" + dadosM[1] + ";"
                                        + dadosM[2];
                                dadosList.add(temp);
                                achou = true;
                         
                                registraTransacao(dadosK[1], Integer.parseInt(aux[2]), Double.parseDouble(dadosM[2]),
                                        aux[1], dadosM[0]);
                            }
                        } else if (Integer.parseInt(dadosM[1]) == Integer.parseInt(aux[2])) {
                            //colocar verificação de preço
                            //System.out.println("8");
                            dadosList.remove(i);
                            registraTransacao(dadosK[1], Integer.parseInt(aux[2]), Double.parseDouble(dadosM[2]),
                                    aux[1], dadosM[0]);
                        }

                    }

                }
            }
        }

        if (!achou) {
            //System.out.println("9");
            //se a quantidade é igual ele passa aqui n sei se quer fazer isso
            dadosList.add(key + ";" + dadosM[0] + ";" + dadosM[1] + ";" + dadosM[2]);
        }
    }

    private static void registraTransacao(String ativo, int quant, double val, String comprador, String vendedor) {
        try {
            FileWriter arquivo = new FileWriter(arqTransacoes, true);
            try {
                String linha = LocalDateTime.now().format(formatador) + ";" + ativo + ";" + quant + ";" + val + ";"
                        + comprador + ";" + vendedor;
                arquivo.write(linha + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
            arquivo.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
