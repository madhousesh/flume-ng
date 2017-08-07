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
package org.apache.flume.sink.hive;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.sink.hive.utils.DeserializeUtils;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.StrictJsonWriter;
import org.apache.hive.hcatalog.streaming.TransactionBatch;

import java.io.IOException;
import java.util.Collection;

public class PerformadRtbJSON implements HiveEventSerializer {
    public static final String ALIAS = "PerformadRtbJSON";
    private String jsonClassName;

    @Override
    public void write(TransactionBatch txnBatch, Event e) throws StreamingException, IOException, InterruptedException {
        DeserializeUtils.deserializeFlumeEventBodyToJson(e,jsonClassName);
        txnBatch.write(e.getBody());
    }



    @Override
    public void write(TransactionBatch txnBatch, Collection<byte[]> events) throws StreamingException, IOException, InterruptedException {
        txnBatch.write(events);
    }

    @Override
    public org.apache.hive.hcatalog.streaming.RecordWriter createRecordWriter(org.apache.hive.hcatalog.streaming.HiveEndPoint endPoint) throws StreamingException, IOException, ClassNotFoundException {
        return new StrictJsonWriter(endPoint);
    }

    @Override
    public void configure(Context context) {
        this.jsonClassName = context.getString("serializer.jsonClassName");
    }

}
