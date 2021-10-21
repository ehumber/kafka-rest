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

package io.confluent.kafkarest.resources;

import static java.util.Objects.requireNonNull;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.confluent.kafkarest.Time;
import io.confluent.kafkarest.config.ConfigModule.ProduceGracePeriodConfig;
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitCacheExpiryConfig;
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitConfig;
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitEnabledConfig;
import io.confluent.kafkarest.exceptions.RateLimitGracePeriodExceededException;
import io.confluent.kafkarest.exceptions.StatusCodeException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.ws.rs.core.Response.Status;

public class RateLimiters {

  private final int maxRequestsPerSecond;
  private final int gracePeriod;
  private final boolean rateLimitingEnabled;
  private final LoadingCache<String, RateLimiter> cache;

  @Inject
  public RateLimiters(
      @ProduceGracePeriodConfig Integer produceGracePeriodConfig,
      @ProduceRateLimitConfig Integer produceRateLimitConfig,
      @ProduceRateLimitEnabledConfig Boolean produceRateLimitEnabledConfig,
      @ProduceRateLimitCacheExpiryConfig Integer produceRateLimitCacheExpiryConfig,
      Time time) {
    this.maxRequestsPerSecond = requireNonNull(produceRateLimitConfig);
    this.gracePeriod = requireNonNull(produceGracePeriodConfig);
    this.rateLimitingEnabled = requireNonNull(produceRateLimitEnabledConfig);
    requireNonNull(time);

    CacheLoader<String, RateLimiter> loader =
        new CacheLoader<String, RateLimiter>() {
          @Override
          public RateLimiter load(String key) {
            return new RateLimiter(gracePeriod, maxRequestsPerSecond, time);
          }
        };

    cache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(produceRateLimitCacheExpiryConfig, TimeUnit.MILLISECONDS)
            .build(loader);
  }

  public Optional<Duration> calculateGracePeriodExceeded(String clusterId)
      throws RateLimitGracePeriodExceededException {
    if (!rateLimitingEnabled) {
      return Optional.empty();
    }
    try {
      RateLimiter rateLimiter = cache.get(clusterId);
      Optional<Duration> waitTime = rateLimiter.calculateGracePeriodExceeded();
      return waitTime;
    } catch (ExecutionException e) {
      throw StatusCodeException.create(
          Status.INTERNAL_SERVER_ERROR,
          "ExecutionException " + "when updating RateLimiter cache",
          e.getMessage());
    }
  }

  public void clear() {
    cache.invalidateAll();
  }
}
