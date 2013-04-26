package org.fusesource.stomp.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.*;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Testing implementation of the Request Reply EAIP using ActiveMQ and Stomp
 *
 * @author zapodot at gmail dot com
 */
public class ActiveMQJmsStompRequestReplyTest  {

    public static final String TOPIC_NAME = "topic";
    public static final String BODY_ANSWER = "answer";
    public static final String REPLY_DESTINATION = "replyDestination";
    public static final String JMS_CORRELATION_ID = "correlationId";
    BrokerService broker;
    int stompPort;
    Session sessionForService;
    private MessageConsumer topicConsumer;
    private int openWirePort;


    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        final TransportConnector stompConnector = broker.addConnector("stomp://0.0.0.0:0");
        final TransportConnector openWireConnector = broker.addConnector("tcp://0.0.0.0:0");
        openWirePort = openWireConnector.getConnectUri().getPort();
        broker.start();
        broker.waitUntilStarted();
        stompPort = stompConnector.getConnectUri().getPort();
        final ConnectionFactory connectionFactoryForService = createOpenWireConnectionFactory();
        final Connection connection = connectionFactoryForService.createConnection();
        sessionForService = connection.createSession(false,
                                                     Session.AUTO_ACKNOWLEDGE);
        final Topic topic = sessionForService.createTopic(TOPIC_NAME);
        topicConsumer = sessionForService.createConsumer(topic);
        topicConsumer.setMessageListener(new MessageListener() {
            public void onMessage(final Message message) {
                try {
                    final TextMessage responseMessage = sessionForService.createTextMessage(BODY_ANSWER);
                    responseMessage.setJMSCorrelationID(message.getJMSCorrelationID());
                    final Destination replyToDestination = message.getJMSReplyTo();
                    final MessageProducer producer = sessionForService.createProducer(replyToDestination);
                    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                    producer.send(responseMessage);
                    producer.close();
                } catch (JMSException e) {
                    fail(e.getMessage());
                }
            }
        });
        connection.start();


    }


    @Test
    public void testRequestReplyStomp() throws Exception {
        final Connection connection = createStompConnectionFactory().createConnection();
        doRequestReplyTest(connection);

    }

    @Test
    public void testRequestReplyOpenWire() throws Exception {
        final Connection connection = createOpenWireConnectionFactory().createConnection();
        doRequestReplyTest(connection);

    }

    private void doRequestReplyTest(final Connection connection) throws JMSException {
        Session session = null;
        final String correlationId = UUID.randomUUID().toString();
        try {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Destination replyDestination = session.createTemporaryQueue();
            System.out.println(replyDestination.toString());
            final MessageConsumer replyQueueConsumer = session.createConsumer(replyDestination, String.format("JMSCorrelationID='%1$s'", correlationId));

            final TextMessage message = session.createTextMessage("body");
            message.setJMSCorrelationID(correlationId);
            message.setJMSReplyTo(replyDestination);

            connection.start();

            final MessageProducer producer = session.createProducer(session.createTopic(TOPIC_NAME));
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            producer.send(message);
            assertTextMessageReceived(BODY_ANSWER, replyQueueConsumer);
            replyQueueConsumer.close();
            producer.close();
        } finally {
            if(session != null) {
                session.close();
            }
            connection.close();

        }
    }

    protected ConnectionFactory createOpenWireConnectionFactory() {
        return new ActiveMQConnectionFactory("tcp://localhost:" + this.openWirePort);
    }

    protected ConnectionFactory createStompConnectionFactory() throws Exception {
        StompJmsConnectionFactory result = new StompJmsConnectionFactory();
        result.setBrokerURI("tcp://localhost:" + this.stompPort);
        return result;
    }

    @After
    public void tearDown() throws Exception {
        topicConsumer.close();
        sessionForService.close();

        broker.stop();
        broker.waitUntilStopped();

    }

    private void assertTextMessageReceived(String expected, MessageConsumer sub) throws JMSException {
        Message msg = sub.receive(TimeUnit.SECONDS.convert(10, TimeUnit.MILLISECONDS));
        assertNotNull("A message was not received.", msg);
        assertEquals(expected, ((TextMessage)msg).getText());
    }
}
