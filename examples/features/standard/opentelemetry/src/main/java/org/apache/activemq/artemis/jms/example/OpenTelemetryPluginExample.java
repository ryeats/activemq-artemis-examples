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
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.baggage.BaggageEntryMetadata;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.apache.qpid.jms.JmsConnectionFactory;

/**
 * A simple example which shows how to use a QueueBrowser to look at messages of a queue without removing them from the queue
 */
public class OpenTelemetryPluginExample {
   private static OpenTelemetrySdk sdk = initopentelemetry();
   //   private static Tracer openTracer = OpenTracingShim.createTracerShim(GlobalOpenTelemetry.get());
      private static io.opentelemetry.api.trace.Tracer telemTracer = GlobalOpenTelemetry.getTracer(OpenTelemetryPlugin.class.getName());
      private static TextMapSetter<Message> telemSetter = new JmsMessageSetter();
      private static TextMapGetter<Message> telemGetter = new JmsMessageGetter();

   public static void main(final String[] args) throws Exception {
      // This example will send and receive an AMQP message
      sendConsumeAMQP();

      // And it will also send and receive a Core message
//      sendConsumeCore();
      //Need to give the rpc call that sends the spans some time to complete or we won't see the full trace
      Thread.sleep(3000);
   }

   private static void sendConsumeAMQP() throws JMSException {

      Connection connection = null;
      JmsConnectionFactory connectionFactory = new JmsConnectionFactory("amqp://localhost:5672");
//      JmsConnectionFactory connectionFactory = new JmsConnectionFactory("amqp://localhost:5672?jms.tracing=opentracing");
      //      connectionFactory.setTracer(OpenTracingTracerFactory.create(openTracer));
      try {

         // Create an amqp qpid 1.0 connection
         connection = connectionFactory.createConnection();

         // Create a session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Create a sender
         //         Queue queue = session.createQueue("exampleQueue");
         Topic topic = session.createTopic("exampleTopic");
         //         MessageProducer sender = session.createProducer(queue);
         MessageProducer sender = session.createProducer(topic);

         // send a few simple message

         connection.start();

         // create a moving receiver, this means the message will be removed from the queue
         MessageConsumer consumer = session.createConsumer(topic);
         MessageConsumer consumer2 = session.createConsumer(topic);
         Baggage baggage = Baggage.builder()
            .put("test1", "value1", BaggageEntryMetadata.create("propagation=unlimited"))
            .put("test2", "value2")
            .build();
         Context contextWithBaggage = baggage.storeInContext(Context.current());

         // Make the context with baggage current
         try (Scope scope = contextWithBaggage.makeCurrent()) {
            Span span = telemTracer.spanBuilder("amqpSend").setSpanKind(SpanKind.PRODUCER).startSpan();
            try (Scope spanScope = span.makeCurrent()) {
            TextMessage message = session.createTextMessage("Hello world ");
            GlobalOpenTelemetry.get().getPropagators().getTextMapPropagator().inject(io.opentelemetry.context.Context.current(), message, telemSetter);
            sender.send(message);
            } finally {
               span.end();
            }
         }

         // receive the simple message
         Message receive = consumer.receive(5000);
         //TODO this extracts traceparent from the Application-properties it appears qpid itself expects spans to be in the message-annotations but artemis only supports putting it in the delivery annotations
         //TODO qpid only support opentrace and hasn't been migrated to oepntelemetry or added support for brave
         Context extractedContext = GlobalOpenTelemetry.get().getPropagators().getTextMapPropagator()
            .extract(Context.current(), receive, telemGetter);
         Span receiveSpan = telemTracer.spanBuilder("AmqpReceive").setParent(extractedContext).setSpanKind(SpanKind.CONSUMER).startSpan();
         receiveSpan.end();
         //  DO WORK
         Thread.sleep(100);
         Message receive2 = consumer2.receive(5000);
         extractedContext = GlobalOpenTelemetry.get().getPropagators().getTextMapPropagator()
            .extract(Context.current(), receive2, telemGetter);
         receiveSpan = telemTracer.spanBuilder("AmqpReceive2").setParent(extractedContext).setSpanKind(SpanKind.CONSUMER).startSpan();
         receiveSpan.end();

      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      } finally {
         if (connection != null) {
            // close the connection
            connection.close();
         }
      }
   }


   private static void sendConsumeCore() throws JMSException {
      Connection connection = null;
      try {
         // Perform a lookup on the Connection Factory
         ConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616");

         Queue queue = new ActiveMQQueue("exampleQueue");

         // Create a JMS Connection
         connection = cf.createConnection();

         // Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);
         Span span = telemTracer.spanBuilder("CoreSend").setSpanKind(SpanKind.PRODUCER).startSpan();
         try (Scope scope = span.makeCurrent()) {
            // Create a Text Message
            TextMessage message = session.createTextMessage("This is a text message");
            GlobalOpenTelemetry.get().getPropagators().getTextMapPropagator().inject(io.opentelemetry.context.Context.current(), message,telemSetter);

            // Send the Message
            producer.send(message);
         }finally {
            span.end();
         }
         io.opentelemetry.sdk.common.CompletableResultCode blah;
         // Create a JMS Message Consumer
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Start the Connection
         connection.start();


         // Receive the message
         Message receivedMsg = messageConsumer.receive(5000);
         Context extractedContext = W3CTraceContextPropagator.getInstance()
            .extract(Context.current(), receivedMsg, telemGetter);
         Span receiveSpan = telemTracer.spanBuilder("CoreReceive").setSpanKind(SpanKind.CONSUMER).setParent(extractedContext).startSpan();
            //  DO WORK
            Thread.sleep(10);
         receiveSpan.end();
      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      } finally {
         if (connection != null) {
            connection.close();
         }
      }

   }
   static class JmsMessageSetter implements TextMapSetter<Message> {

      @Override
      public void set(Message carrier, String key, String value) {
         try {
            carrier.setStringProperty(key, value);
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }
   }
      static class JmsMessageGetter implements TextMapGetter<Message> {
         @Override
         public Iterable<String> keys(Message message) {
            try {
               Enumeration propertyNames = message.getPropertyNames();
               return () -> new Iterator<>() {
                  @Override
                  public boolean hasNext() {
                     return propertyNames.hasMoreElements();
                  }

                  @Override
                  public String next() {
                     return propertyNames.nextElement().toString();
                  }
               };
            } catch (JMSException e) {
               throw new RuntimeException(e);
            }

         }

         @Override
         public String get(Message message, String s) {
            try {
               return message.getStringProperty(s);
            } catch (JMSException e) {
               throw new RuntimeException(e);
            }
         }
      }

   public static OpenTelemetrySdk initopentelemetry() {
      try {
         //TODO get properties from env or xml or broker.profile properties
         InputStream input = OpenTelemetryPlugin.class.getClassLoader().getResourceAsStream("tracing.properties");
         if (input == null) {
            throw new NullPointerException("Unable to find tracing.properties file");
         }
         Properties prop = new Properties(System.getProperties());
         prop.load(input);
         System.setProperties(prop);
         sdk = AutoConfiguredOpenTelemetrySdk.initialize().getOpenTelemetrySdk();

      } catch (Throwable t) {
         t.printStackTrace();
      }
      return sdk;
   }
}
