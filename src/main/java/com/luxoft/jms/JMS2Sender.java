package com.luxoft.jms;

import com.sun.messaging.ConnectionConfiguration;
import com.sun.messaging.ConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.jms.*;

public class JMS2Sender {
    private static volatile int numberOfConfirmations = 0;
    private static int receiverNumber;

    public static void main(String[] args) throws JMSException, InterruptedException {
        Logger logger = LogManager.getLogger(JMS2Sender.class);

        String[] ibans = new String[]{"DE443794221391961944346", "FR143841010373619987209"};
        int[] receiverSumOnAccount = new int[5];

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setProperty(ConnectionConfiguration.imqAddressList, "mq://localhost:7676");
        try (JMSContext jmsContext = connectionFactory.createContext();) {
            Queue queueRequest = jmsContext.createQueue("ACCOUNTS_2.QUEUE");
            Queue queueResponse = jmsContext.createQueue("ACCOUNTS_2.QUEUE.RESP");

            JMSConsumer receiver = jmsContext.createConsumer(queueResponse);
            receiver.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message confirmationMessage) {
                    try {

                        receiverNumber = confirmationMessage.getIntProperty("receiverNumber");
                        receiverSumOnAccount[receiverNumber] = confirmationMessage.getIntProperty("SumOnAccount");
                        logger.info("Confirmation({}) received from receiver {} with total sum : {}", numberOfConfirmations, receiverNumber, receiverSumOnAccount[receiverNumber]);
                        numberOfConfirmations++;
                    } catch (JMSException jmsException) {
                        jmsException.printStackTrace();
                    }
                }
            });

            while (numberOfConfirmations < JMS2Receiver.getFilters().length) {
                int amount = (int) (100 * Math.random());
                TextMessage message = jmsContext.createTextMessage("DEPOSIT " + amount + " EUR into account " + ibans[amount % 2]);

                jmsContext.createProducer()
                        .setProperty("iban", ibans[amount % 2])
                        .setProperty("amount", amount)
                        .setJMSReplyTo(queueResponse)
                        .send(queueRequest, message);
                logger.info("Confirmation message send: {}", message.getText());

            }
        }
    }


}