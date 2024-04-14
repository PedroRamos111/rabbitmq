Dupla: 

- Giovanni Duarte;
- Pedro Ramos Vidgal.




Instruções para a compilação:
1. Executar o arquivo Bolsa.java
2. Abrir um novo terminal e executar o arquivo Broker.java

Explicação do código:

O código é feito em java, contendo 2 classes, Bolsa e Broker. 

O Broker está encarregado de enviar e receber mensagens da bolsa, enviando uma mensagem de pedidos de compra e venda, e recebendo atualizações sobre as ações escolhidas. O código do broker implementa a interface Runnable para enviar e receber as mensagens em threads e contem um método para ver qual ação será recebida as mensagens e um método para escolher o conteúdo das mensagens à ser enviada. 

Já a bolsa está encarregada de receber os pedidos, enviar as atualizações das ações, descobrir se um pedido de venda ou compra coincide com outro pedido para gerar uma transação. O código da bolsa utiliza threads para receber e enviar as mensagens, também contem um método para registrar os dados de pedidos, um método para registrar os dados de transações, e um método para checar se os pedidos podem se transformar em transações, fazendo as alterações necessárias.
