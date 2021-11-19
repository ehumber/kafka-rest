/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafkarest.resources.v3;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.confluent.kafkarest.config.ConfigModule.ProduceResponseThreadPoolSizeConfig;
import io.confluent.kafkarest.resources.RateLimiter;
import io.confluent.kafkarest.response.ChunkedOutputFactory;
import io.confluent.kafkarest.response.StreamingResponseFactory;
import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.glassfish.hk2.api.AnnotationLiteral;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

public final class V3ResourcesModule extends AbstractBinder {

  @Override
  protected void configure() {
    bindAsContract(RateLimiter.class).in(Singleton.class);
    bindAsContract(ChunkedOutputFactory.class);
    bindAsContract(StreamingResponseFactory.class);
    bind(Clock.systemUTC()).to(Clock.class);
    bindFactory(ProduceResponseExecutorServiceFactory.class)
        .to(ExecutorService.class)
        .in(Singleton.class);
  }

  public @interface ProduceResponseExecutorService {}

  private static final class ProduceResponseExecutorServiceFactory
      extends AnnotationLiteral<ProduceResponseExecutorService>
      implements ProduceResponseExecutorService, Factory<ExecutorService> {

    private final int produceResponseThreadPoolSize;

    @Inject
    ProduceResponseExecutorServiceFactory(
        @ProduceResponseThreadPoolSizeConfig Integer produceExecutorThreadPoolSize) {
      this.produceResponseThreadPoolSize = produceExecutorThreadPoolSize;
    }

    @Override
    public ExecutorService provide() {
      ThreadFactory namedThreadFactory =
          new ThreadFactoryBuilder().setNameFormat("Produce-response-thread-%d").build();
      return Executors.newFixedThreadPool(produceResponseThreadPoolSize, namedThreadFactory);
    }

    @Override
    public void dispose(ExecutorService executorService) {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        executorService.shutdownNow();
      }
    }
  }
}
