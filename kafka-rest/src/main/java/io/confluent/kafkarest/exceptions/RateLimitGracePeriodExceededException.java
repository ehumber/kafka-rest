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

package io.confluent.kafkarest.exceptions;

import javax.ws.rs.core.Response.Status;

public final class RateLimitGracePeriodExceededException extends StatusCodeException {

  public RateLimitGracePeriodExceededException(int rateLimit, int gracePeriod) {
    super(
        Status.TOO_MANY_REQUESTS,
        "Rate limit of "
            + rateLimit
            + " messages per second exceeded within the grace period of "
            + gracePeriod
            + " ms ",
        "connection will be closed.");
  }
}
