/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.jms.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsConnectionFactory;

import java.security.SecureRandom;
import java.util.Random;

/**
 * This example is demonstrating how messages are federated between two brokers with the
 * federation configuration located on only one broker (server0) and only a single outbound
 * connection is configured from server0 to server1
 */
public class BrokerFederationExample {

   public static void main(final String[] args) throws Exception {
      long seed = new SecureRandom().nextLong();
      System.err.println("SEED:"+seed);
      Random random = new Random(seed);
      final ConnectionFactory connectionFactoryServer0;
      final ConnectionFactory connectionFactoryServer1;

      switch (random.nextInt()%4)
      {
         case 0:
            System.err.println("Openwire connecting to 5660");
            connectionFactoryServer0 = new org.apache.activemq.ActiveMQConnectionFactory("tcp://localhost:5660");
            break;
         case 1:
            System.err.println("Core connecting to 5660");
            connectionFactoryServer0 = new ActiveMQConnectionFactory("tcp://localhost:5660");
            break;
         case 2:
            System.err.println("AMQP connecting to 5660");
            connectionFactoryServer0 = new JmsConnectionFactory("amqp://localhost:5660");
            break;
         case 4:
            System.err.println("STOMP connecting to 5660");
            StompJmsConnectionFactory stompJmsConnectionFactory = new StompJmsConnectionFactory();
            stompJmsConnectionFactory.setBrokerURI("tcp://localhost:5660");
            connectionFactoryServer0 = stompJmsConnectionFactory;
            break;
          default:
             connectionFactoryServer0 = new JmsConnectionFactory("amqp://localhost:5660");
             break;
      }
      switch (random.nextInt()%4)
      {
         case 0:
            System.err.println("Openwire connecting to 5771");
            connectionFactoryServer1 = new org.apache.activemq.ActiveMQConnectionFactory("tcp://localhost:5771");
            break;
         case 1:
            System.err.println("Core connecting to 5771");
            connectionFactoryServer1 = new ActiveMQConnectionFactory("tcp://localhost:5771");
            break;
         case 2:
            System.err.println("AMQP connecting to 5771");
            connectionFactoryServer1 = new JmsConnectionFactory("amqp://localhost:5771");
            break;
         case 4:
            System.err.println("STOMP connecting to 5771");
            StompJmsConnectionFactory stompJmsConnectionFactory = new StompJmsConnectionFactory();
            stompJmsConnectionFactory.setBrokerURI("tcp://localhost:5771");
            connectionFactoryServer1 = stompJmsConnectionFactory;
            break;
         default:
            connectionFactoryServer1 = new JmsConnectionFactory("amqp://localhost:5771");
            break;
      }
      final Connection connectionOnServer0 = connectionFactoryServer0.createConnection();
      final Connection connectionOnServer1 = connectionFactoryServer1.createConnection();

      connectionOnServer0.start();
      connectionOnServer1.start();

      final Session sessionOnServer0 = connectionOnServer0.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final Session sessionOnServer1 = connectionOnServer1.createSession(false, Session.AUTO_ACKNOWLEDGE);

      final Destination orders0;
      final Destination orders1;
      switch (random.nextInt()%6){
         case 1:
            System.err.println("creating order queues");
            orders0 = sessionOnServer0.createQueue("orders");
            orders1 = sessionOnServer1.createQueue("orders");
            break;
         case 2:
            System.err.println("creating order.divert topics");
            orders0 = sessionOnServer0.createTopic("orders");
            orders1 = sessionOnServer1.createTopic("orders.divert");
            break;
         case 3:
            System.err.println("creating order.divert queues");
            orders0 = sessionOnServer0.createQueue("orders");
            orders1 = sessionOnServer1.createQueue("orders.divert");
            break;
         case 4:
            System.err.println("creating order.divert topics");
            orders0 = sessionOnServer0.createTopic("orders.divert");
            orders1 = sessionOnServer1.createTopic("orders");
            break;
         case 5:
            System.err.println("creating order.divert queues");
            orders0 = sessionOnServer0.createQueue("orders.divert");
            orders1 = sessionOnServer1.createQueue("orders");
            break;
         default:
            System.err.println("creating order topics");
            orders0 = sessionOnServer0.createTopic("orders");
            orders1 = sessionOnServer1.createTopic("orders");
            break;
      }

      final Destination tracking0;
      final Destination tracking1;
      switch (random.nextInt()%6){
         case 0:
            System.err.println("creating tracking topics");
            tracking0 = sessionOnServer0.createTopic("tracking");
            tracking1 = sessionOnServer1.createTopic("tracking");
            break;
         case 2:
            System.err.println("creating tracking.divert topics");
            tracking0 = sessionOnServer0.createTopic("tracking");
            tracking1 = sessionOnServer1.createTopic("tracking.divert");
            break;
         case 3:
            System.err.println("creating tracking.divert queues");
            tracking0 = sessionOnServer0.createQueue("tracking");
            tracking1 = sessionOnServer1.createQueue("tracking.divert");
            break;
         case 4:
            System.err.println("creating tracking.divert topics");
            tracking0 = sessionOnServer0.createTopic("tracking.divert");
            tracking1 = sessionOnServer1.createTopic("tracking");
            break;
         case 5:
            System.err.println("creating tracking.divert queues");
            tracking0 = sessionOnServer0.createQueue("tracking.divert");
            tracking1 = sessionOnServer1.createQueue("tracking");
            break;
         default:
            System.err.println("creating tracking queues");
            tracking0 = sessionOnServer0.createQueue("tracking");
            tracking1 = sessionOnServer1.createQueue("tracking");
            break;
      }

      // Create consumers which generate demand on tracked resources and create federation links
      final MessageConsumer ordersConsumerOn0 = sessionOnServer0.createConsumer(orders0);
      final MessageConsumer trackingConsumerOn1 = sessionOnServer1.createConsumer(tracking1);
      Thread.sleep(2000);

      // Federation from server0 to server1 on the tracking queue
      final MessageProducer trackingProducerOn0 = sessionOnServer0.createProducer(tracking0);

      final TextMessage trackingMessageSent = sessionOnServer0.createTextMessage("new-tracking-data");

      trackingProducerOn0.send(trackingMessageSent);

      final TextMessage trackingMessageReceived = (TextMessage) trackingConsumerOn1.receive(5_000);

      System.out.println("Consumer on server 1 received tracking data from producer on server 0 " + trackingMessageReceived.getText());

      // Federation from server1 back to server0 on the orders address
      final MessageProducer ordersProducerOn1 = sessionOnServer1.createProducer(orders1);

      final TextMessage orderMessageSent = sessionOnServer1.createTextMessage("new-order");

      ordersProducerOn1.send(orderMessageSent);

      final TextMessage orderMessageReceived = (TextMessage) ordersConsumerOn0.receive(5_000);

      System.out.println("Consumer on server 0 received order message from producer on server 1 " + orderMessageReceived.getText());
   }
}
