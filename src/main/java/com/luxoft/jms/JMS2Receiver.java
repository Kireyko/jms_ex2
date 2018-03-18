package com.luxoft.jms;

import com.sun.messaging.ConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.jms.*;

public class JMS2Receiver {
    private Logger logger = LogManager.getLogger(JMS2Receiver.class);

    public static String[] getFilters() {
        return filters;
    }

    private static String[] filters = {
            "iban like 'DE%' and (amount <= 25 or amount >=75) ",
            "iban like 'DE%' and (amount > 25 and amount <75 ) ",
            "iban like 'FR%' and (amount <= 25 or amount >=75) ",
            "iban like 'FR%' and (amount > 25 and amount <75 ) "
    };

    private static volatile int receiverNumber;
    private int SumOnAccount;
    private int sendStatus;

    public  JMS2Receiver(String filter) {
        threadReceiver(filter);
    }

    private  void threadReceiver(String filter){
        int recN = receiverNumber;
        receiverNumber++;
        logger.info("[{}] receiver Number ", recN);

        ConnectionFactory connectionFactory = new ConnectionFactory();
        JMSContext jmsContext = connectionFactory.createContext();

        Queue queue = jmsContext.createQueue("ACCOUNTS_2.QUEUE");
        JMSConsumer jmsConsumer = jmsContext.createConsumer(queue, filter);

        jmsConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    logger.info("[{}] Incoming message: {}",recN, ((TextMessage) message).getText());

                    if (SumOnAccount<=100){
                        SumOnAccount = SumOnAccount + ((TextMessage) message).getIntProperty("amount");
                        logger.info("[{}] SumOnAccount:{}",recN, SumOnAccount);
                    }

                    if (SumOnAccount>100 & sendStatus==0) {
                        logger.info("[{}] Sending response ... ", recN);
                        sendStatus=1;

                        String confirmation = message.getBody(String.class) ;
                       // logger.info("Command execution confirmation: {}", confirmation);

                        jmsContext.createProducer()
                                //.setJMSCorrelationID(message.getJMSMessageID())
                                .setProperty("receiverNumber", recN)
                                .setProperty("SumOnAccount", SumOnAccount)
                                .send(message.getJMSReplyTo(), confirmation);
                    }

                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
        logger.info("[{}] Waiting on messages fulfilling the condition: {}",recN, jmsConsumer.getMessageSelector());
    }

    public static void main(String[] args) {
        for (String filterElement: filters) {
            new Thread() {
                public void run() {
                    new JMS2Receiver(filterElement);
                }
            }.start();
        }
    }
}