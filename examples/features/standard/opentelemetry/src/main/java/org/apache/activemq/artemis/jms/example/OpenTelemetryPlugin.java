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

import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.RoutingStatus;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;

public class OpenTelemetryPlugin implements ActiveMQServerPlugin {

   private static final String DELIVER_NAME = "ArtemisMessageDelivery";
   private static final String ROUTE_NAME = "ArtemisMessageRoute";
   private static final String SEND_NAME = "ArtemisMessageSend";
   private static final String ACK_NAME = "ArtemisMessageAck";
   private static final String EXPIRE_NAME = "ArtemisMessageExpire";
   private static final CoreMessageGetter TRACE_GET = new CoreMessageGetter();
   private static final CoreMessageSetter TRACE_SET = new CoreMessageSetter();
   private static final AmqpDeliveryAnnotationsSetter TRACE_DELIVERY_SET = new AmqpDeliveryAnnotationsSetter();
   private static OpenTelemetrySdk sdk = initopentelemetry();
   private static Tracer tracer = GlobalOpenTelemetry.getTracer("Artemis");

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

   @Override
   public void beforeSend(ServerSession session,
                          Transaction tx,
                          Message message,
                          boolean direct,
                          boolean noAutoCreateQueue) throws ActiveMQException {
      Span spanParent = (Span) message.getUserContext(Span.class);
      SpanBuilder spanBuilder = tracer.spanBuilder(SEND_NAME);
      if (spanParent != null) {
         spanParent.makeCurrent();
                  spanBuilder.setParent(Context.current().with(spanParent));
      }else{
         //TODO may need to do additional work for amqp messages using https://w3c.github.io/trace-context-amqp/
         Context parentContext = GlobalOpenTelemetry.get().getPropagators().getTextMapPropagator().extract(Context.current(), message, TRACE_GET);
         parentContext.makeCurrent();
         spanBuilder.setParent(parentContext);
      }
      Span span = spanBuilder.startSpan();
      span.setAttribute("address", message.getAddress());
      message.setUserContext(SEND_NAME, span);
   }

   @Override
   public void afterSend(Transaction tx,
                         Message message,
                         boolean direct,
                         boolean noAutoCreateQueue,
                         RoutingStatus result) throws ActiveMQException {
      Span span = (Span) message.getUserContext(SEND_NAME);
      span.end();
   }

   @Override
   public void onSendException(ServerSession session,
                               Transaction tx,
                               Message message,
                               boolean direct,
                               boolean noAutoCreateQueue,
                               Exception e) throws ActiveMQException {
      Span span = (Span) message.getUserContext(Span.class);
      span.setStatus(StatusCode.ERROR).recordException(e);
      span.end();
   }

   @Override
   public void beforeMessageRoute(Message message, RoutingContext context, boolean direct, boolean rejectDuplicates) throws ActiveMQException {
      Span spanParent = (Span) message.getUserContext(ROUTE_NAME);
      SpanBuilder spanBuilder = tracer.spanBuilder(ROUTE_NAME);
      if (spanParent != null) {
         spanParent.makeCurrent();
         spanBuilder.setParent(Context.current().with(spanParent));
      }else{
         Context parentContext = GlobalOpenTelemetry.get().getPropagators().getTextMapPropagator().extract(Context.current(), message, TRACE_GET);
         parentContext.makeCurrent();
         spanBuilder.setParent(parentContext);
      }
      Span span = spanBuilder.startSpan();
      message.setUserContext(ROUTE_NAME, span);
   }

   @Override
   public void afterMessageRoute(Message message, RoutingContext context, boolean direct, boolean rejectDuplicates,
                                  RoutingStatus result) throws ActiveMQException {
      Span span = (Span) message.getUserContext(ROUTE_NAME);
      span.end();
   }
   @Override
   public void onMessageRouteException(Message message, RoutingContext context, boolean direct, boolean rejectDuplicates,
                                        Exception e) throws ActiveMQException {
      Span span = (Span) message.getUserContext(ROUTE_NAME);
      span.setStatus(StatusCode.ERROR).recordException(e);
      span.end();

   }

   @Override
   public void beforeDeliver(ServerConsumer consumer, MessageReference ref) throws ActiveMQException {
      Span parent = (Span) ref.getMessage().getUserContext(SEND_NAME);
      SpanBuilder spanBuilder = tracer.spanBuilder(DELIVER_NAME);
      if (parent != null) {
         parent.makeCurrent();
         spanBuilder.setParent(Context.current().with(parent));
      }
      Span span = spanBuilder.startSpan();
      ref.setProtocolData(Span.class,span);
      ref.setProtocolData(Context.class,Context.current().with(span));
      if("AMQP".equals(consumer.getConnectionProtocolName())) {
         DeliveryAnnotations deliveryAnnotations = new DeliveryAnnotations(new HashMap<>());
         GlobalOpenTelemetry.get().getPropagators().getTextMapPropagator().inject(Context.current().with(span), deliveryAnnotations, TRACE_DELIVERY_SET);
         ref.setProtocolData(DeliveryAnnotations.class, deliveryAnnotations);
      }
      //TODO only have a single message referenced by multiple consumers so we can't modify the message here and interceptors currently don't have info necessary to pass delivery specific information to them
   }

   @Override
   public void afterDeliver(ServerConsumer consumer, MessageReference ref) throws ActiveMQException {
      Span span =  ref.getProtocolData(Span.class);
      span.end();
   }

   @Override
   public void messageAcknowledged(Transaction tx, MessageReference ref, AckReason reason, ServerConsumer consumer) {
      Span spanParent = ref.getProtocolData(Span.class);
      Span span = tracer.spanBuilder(ACK_NAME).setParent(Context.current().with(spanParent)).startSpan();
      span.end();
   }

   @Override
   public void messageExpired(MessageReference ref,
                              SimpleString messageExpiryAddress,
                              ServerConsumer consumer) throws ActiveMQException {
      Span spanParent = ref.getProtocolData(Span.class);
      Span span = tracer.spanBuilder(EXPIRE_NAME).setParent(Context.current().with(spanParent)).startSpan();
      span.end();
   }



}