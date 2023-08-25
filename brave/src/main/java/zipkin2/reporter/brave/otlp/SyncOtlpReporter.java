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

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.util.JsonFormat;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.TracesData;
import zipkin2.codec.Encoding;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.Reporter;
import zipkin2.reporter.Sender;

/**
 * Synchronous reporter of {@link ResourceSpans}. Uses a given {@link Sender}
 * to actually send the spans over the wire. Supports {@link Encoding#JSON}
 * and {@link Encoding#PROTO3} formats.
 *
 * // TODO: Think of an async version, currently {@link AsyncReporter} is unusable
 *
 * @since 2.16
 */
public final class SyncOtlpReporter implements Reporter<TracesData>, Closeable {
  static final Logger logger = Logger.getLogger(SyncOtlpReporter.class.getName());

  private final Sender sender;

  SyncOtlpReporter(Sender sender) {
    this.sender = sender;
  }

  /**
   * Creates a new instance of the {@link SyncOtlpReporter}.
   *
   * @param sender sender to send spans
   * @return {@link Reporter}
   * @since 2.16
   */
  public static Reporter<TracesData> create(Sender sender) {
    return new SyncOtlpReporter(sender);
  }

  @Override
  public void report(TracesData tracesData) {
    try {
      Encoding encoding = this.sender.encoding();
      byte[] bytes;
      if (encoding == Encoding.JSON) {
        String asString = JsonFormat.printer()
          .printingEnumsAsInts()
          .print(tracesData);
        // TODO: https://stackoverflow.com/questions/53080136/protobuf-jsonformater-printer-convert-long-to-string-in-json
        // this is ridiculous...
        asString = modifyBrokenJson(asString);
        bytes = asString.getBytes(Charset.defaultCharset());
      }
      else {
        throw new UnsupportedOperationException("You can send spans only in JSON format");
      }
      // TODO: Currently we have 1 span per ResourceSpans, we could batch them
      sender.sendSpans(bytes).execute();
    }
    catch (IOException e) {
      logger.log(Level.WARNING, "Exception occurred while trying to send spans", e);
      throw new RuntimeException(e);
    }
  }

  private String modifyBrokenJson(String asString) {
    // "startTimeUnixNano", "endTimeUnixNano", "timeUnixNano" are wrong
    DocumentContext documentContext = JsonPath.parse(asString);
    documentContext.map("$.resourceSpans[*].scopeSpans[*].spans[*].startTimeUnixNano", SyncOtlpReporter::convertToNumber);
    documentContext.map("$.resourceSpans[*].scopeSpans[*].spans[*].endTimeUnixNano", SyncOtlpReporter::convertToNumber);
    documentContext.map("$.resourceSpans[*].scopeSpans[*].spans[*].events[*].timeUnixNano", SyncOtlpReporter::convertToNumber);
    return documentContext.jsonString();
  }

  private static Object convertToNumber(Object currentValue, Configuration configuration) {
    if (currentValue instanceof String) {
      return Long.valueOf((String) currentValue);
    }
    return currentValue;
  }

  @Override
  public void close() {
    try {
      this.sender.close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
