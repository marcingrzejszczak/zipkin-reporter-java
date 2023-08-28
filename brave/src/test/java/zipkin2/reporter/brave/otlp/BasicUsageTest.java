/*
 * Copyright 2016-2020 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.reporter.brave.otlp;

import brave.Span;
import brave.Span.Kind;
import brave.Tracer;
import brave.Tracing;
import brave.handler.SpanHandler;
import brave.propagation.ThreadLocalCurrentTraceContext;
import brave.sampler.Sampler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import zipkin2.reporter.urlconnection.URLConnectionSender;

class BasicUsageTest {

  SpanHandler spanHandler = OtlpSpanHandler
    .create(SyncOtlpReporter.create(URLConnectionSender.create("http://localhost:4318/v1/traces")));

  ThreadLocalCurrentTraceContext braveCurrentTraceContext = ThreadLocalCurrentTraceContext.newBuilder()
    .build();

  Tracing tracing = Tracing.newBuilder()
    .currentTraceContext(this.braveCurrentTraceContext)
    .supportsJoin(false)
    .traceId128Bit(true)
    .sampler(Sampler.ALWAYS_SAMPLE)
    .addSpanHandler(this.spanHandler)
    .localServiceName("my-service")
    .build();

  Tracer braveTracer = this.tracing.tracer();


  @Test
  void shouldSendSpansToOtlpEndpoint() throws InterruptedException {
    Span started = braveTracer.nextSpan().name("foo")
      .tag("foo tag", "foo value")
      .kind(Kind.CONSUMER)
      .error(new RuntimeException("BOOOOOM!"))
      .remoteServiceName("remote service")
      .start();
    System.out.println("Trace Id <" + started.context().traceIdString() + ">");
    started.remoteIpAndPort("http://localhost", 123456);
    Thread.sleep(1000);
    started.annotate("boom!");
    Thread.sleep(1000);
    started.finish();
    Thread.sleep(1000);
  }

  @AfterEach
  void shutdown() {
    tracing.close();
  }

}
