/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.jms.example;

import java.util.stream.Collectors;

import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;

class CoreMessageGetter implements TextMapGetter<Message> {

   @Override
   public Iterable<String> keys(Message message) {
      return message.getPropertyNames().stream().map(SimpleString::toString).collect(Collectors.toSet());

   }

   @Override
   public String get(Message message, String s) {
      return message.getStringProperty(s);
   }
}
