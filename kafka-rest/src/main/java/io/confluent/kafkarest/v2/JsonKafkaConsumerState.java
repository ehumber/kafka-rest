/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafkarest.ConsumerInstanceId;
import io.confluent.kafkarest.ConsumerRecordAndSize;
import io.confluent.kafkarest.KafkaRestConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonKafkaConsumerState extends KafkaConsumerState<byte[], byte[], Object, Object> {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  public JsonKafkaConsumerState(KafkaRestConfig config,
      ConsumerInstanceId instanceId,
      Consumer consumer) {
    super(config, instanceId, consumer);
  }

  private static final Logger log = LoggerFactory.getLogger(JsonKafkaConsumerState.class);

  @Override
  public ConsumerRecordAndSize<Object, Object> createConsumerRecord(
      ConsumerRecord<byte[], byte[]> record) {
    log.info("** createConsumerRecord entry");
    long approxSize = 0;

    Object key = null;
    Object value = null;

    // The extra serialization here is unfortunate. We could use @JsonRawValue
    // and just use the raw bytes, but that risks returning invalid data to the user
    // if their data is not actually JSON encoded.

    if (record.key() != null) {
      approxSize += record.key().length;
      key = deserialize(record.key());
    }

    if (record.value() != null) {
      approxSize += record.value().length;
      value = deserialize(record.value());
    }

    log.info("** createConsumerRecord exit");
    return new ConsumerRecordAndSize<>(
        io.confluent.kafkarest.entities.ConsumerRecord.create(
            record.topic(), key, value, record.partition(), record.offset()), approxSize);
  }

  private Object deserialize(byte[] data) {
    try {
      return objectMapper.readValue(data, Object.class);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }
}
