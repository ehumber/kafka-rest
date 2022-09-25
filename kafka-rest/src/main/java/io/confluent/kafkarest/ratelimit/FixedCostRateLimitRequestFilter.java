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

package io.confluent.kafkarest.ratelimit;

import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.extension.ResourceAccesslistFeature;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ContainerRequestFilter} that automatically applies a request rate-limit at a fixed cost
 * based on the resource/method being requested, according to the {@link
 * io.confluent.kafkarest.KafkaRestConfig#RATE_LIMIT_COSTS_CONFIG} and {@link
 * io.confluent.kafkarest.KafkaRestConfig#RATE_LIMIT_DEFAULT_COST_CONFIG} configs.
 */
final class FixedCostRateLimitRequestFilter implements ContainerRequestFilter {
  private final FixedCostRateLimiter rateLimiter;

  FixedCostRateLimitRequestFilter(FixedCostRateLimiter rateLimiter) {
    this.rateLimiter = requireNonNull(rateLimiter);
  }

  private static final Logger log = LoggerFactory.getLogger(ResourceAccesslistFeature.class);

  @Override
  public void filter(ContainerRequestContext requestContext) {
    log.error("ELH *** resource accesslist feature");
    log.error("class of request{}", requestContext.getRequest().getClass());
    rateLimiter.rateLimit();
  }
}
