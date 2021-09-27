/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.kafkarest.controllers;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.SystemTime;
import io.confluent.kafkarest.Time;
import io.confluent.kafkarest.entities.ProduceResult;
import io.confluent.kafkarest.exceptions.RateLimitGracePeriodExceededException;
import io.confluent.kafkarest.exceptions.TooManyRequestsException;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

final class ProduceControllerImpl implements ProduceController {

  @VisibleForTesting private final Producer<byte[], byte[]> producer;
  private final KafkaRestConfig config;

  @VisibleForTesting
  static final ConcurrentLinkedQueue<Long> rateCounter = new ConcurrentLinkedQueue<>();

  @VisibleForTesting static Optional<AtomicLong> gracePeriodStart = Optional.empty();
  private static final int ONE_SECOND = 1000;

  @VisibleForTesting public Time time = new SystemTime();

  static final Set<CompletableFuture<ProduceResult>> outstandingRequests =
      ConcurrentHashMap.newKeySet();

  @Inject
  ProduceControllerImpl(Producer<byte[], byte[]> producer, KafkaRestConfig config) {
    this.producer = requireNonNull(producer);
    this.config = config;
  }

  private void addToAndCullRateCounter(long now) {

    rateCounter.add(now);
    while (rateCounter.peek() < now - ONE_SECOND) {
      rateCounter.poll();
    }
    if (rateCounter.size() <= config.getInt(KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND)
        && outstandingRequests.size()
            <= config.getInt(KafkaRestConfig.PRODUCE_MAX_OUTSTANDING_RESPONSES)) {
      gracePeriodStart = Optional.empty();
    }
  }

  @Override
  public CompletableFuture<ProduceResult> produce(
      String clusterId,
      String topicName,
      Optional<Integer> partitionId,
      Multimap<String, Optional<ByteString>> headers,
      Optional<ByteString> key,
      Optional<ByteString> value,
      Instant timestamp) {

    long now = time.milliseconds();
    addToAndCullRateCounter(now);

    CompletableFuture<ProduceResult> result = new CompletableFuture<>();
    if (rateCounter.size() > config.getInt(KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND)
        || outstandingRequests.size()
            > config.getInt(KafkaRestConfig.PRODUCE_MAX_OUTSTANDING_RESPONSES)) {

      if (!gracePeriodStart.isPresent()
          && config.getInt(KafkaRestConfig.PRODUCE_GRACE_PERIOD) != 0) {
        gracePeriodStart = Optional.of(new AtomicLong(now));
      } else if (config.getInt(KafkaRestConfig.PRODUCE_GRACE_PERIOD) == 0
          || config.getInt(KafkaRestConfig.PRODUCE_GRACE_PERIOD)
              < now - gracePeriodStart.get().get()) {
        result.completeExceptionally(new RateLimitGracePeriodExceededException());
        return result;
      }

    } else {
      gracePeriodStart = Optional.empty();
    }

    outstandingRequests.add(result);

    producer.send(
        new ProducerRecord<>(
            topicName,
            partitionId.orElse(null),
            timestamp.toEpochMilli(),
            key.map(ByteString::toByteArray).orElse(null),
            value.map(ByteString::toByteArray).orElse(null),
            headers.entries().stream()
                .map(
                    header ->
                        new RecordHeader(
                            header.getKey(),
                            header.getValue().map(ByteString::toByteArray).orElse(null)))
                .collect(Collectors.toList())),
        (metadata, exception) -> {
          outstandingRequests.remove(result);

          if (exception != null) {
            result.completeExceptionally(exception);
          } else if (gracePeriodStart.isPresent()) {
            result.completeExceptionally(
                new TooManyRequestsException(
                    config.getInt(KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND),
                    config.getInt(KafkaRestConfig.PRODUCE_GRACE_PERIOD)));
          } else {
            result.complete(ProduceResult.fromRecordMetadata(metadata));
          }
        });
    return result;
  }
}
