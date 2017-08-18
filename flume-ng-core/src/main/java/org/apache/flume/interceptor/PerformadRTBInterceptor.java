/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.tools.DeserializeUtils;

import java.util.List;

/**
 * Simple Interceptor class that sets the current system timestamp on all events
 * that are intercepted.
 * By convention, this timestamp header is named "timestamp" and its format
 * is a "stringified" long timestamp in milliseconds since the UNIX epoch.
 */
public class PerformadRTBInterceptor implements Interceptor {

    private final String jsonClassName;

    /**
     * Only {@link PerformadRTBInterceptor.Builder} can build me
     */
    private PerformadRTBInterceptor(String jsonClassName) {
        this.jsonClassName = jsonClassName;
    }

    @Override
    public void initialize() {
        // no-op
    }

    /**
     * Modifies events in-place.
     */
    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        DeserializeUtils.deserializeFlumeEventBodyToJson(event, jsonClassName);
        event.setBody(body);
        return event;
    }

    /**
     * Delegates to {@link #intercept(Event)} in a loop.
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {
        // no-op
    }

    /**
     * Builder which builds new instances of the TimestampInterceptor.
     */
    public static class Builder implements Interceptor.Builder {

        private String jsonClassName;

        @Override
        public Interceptor build() {
            return new PerformadRTBInterceptor(jsonClassName);
        }

        @Override
        public void configure(Context context) {
            jsonClassName = context.getString("jsonClassName");
        }

    }
}
