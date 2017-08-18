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
 * Unless required by applicable law or agreed to in writing, software;
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.sink.hive.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Event;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DeserializeUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DeserializeUtils.class);

    public static void deserializeFlumeEventBodyToJson(Event event, String className) {
        if (StringUtils.isNotEmpty(className)) {
            try {
                String eventBody = new String(event.getBody());
                String jsonString = deserializeToTBaseJson(eventBody, className);
                // 转json对象
                JSONObject jsonObject = JSON.parseObject(jsonString);
                // 获取time字段
                Long time = 0L;
                if (className.toLowerCase().endsWith("adstats")) {
                    Map totalRequestsMap = jsonObject.getObject("hourTotalRequestsMap", Map.class);
                    if (null != totalRequestsMap && totalRequestsMap.size() > 0) {
                        time = Long.valueOf(totalRequestsMap.keySet().toArray()[0].toString());
                    } else {
                    }
                } else {
                    if (null != jsonObject.getLong("time") && jsonObject.getLong("time") > 0L) {
                        time = jsonObject.getLong("time");
                    } else if (null != jsonObject.getLong("timestamp") && jsonObject.getLong("timestamp") > 0L) {
                        time = jsonObject.getLong("timestamp");
                    } else if (null != jsonObject.getLong("eventTime") && jsonObject.getLong("eventTime") > 0L) {
                        time = jsonObject.getLong("eventTime");
                    }
                }
                // 新增字段
                jsonObject.put("timestamp", null == time ? 0L : time);
                // 设置event header，使用记录中的时间作为时间戳
                event.getHeaders().put("timestamp", String.valueOf(null == time ? "0" : time));
                // 将event body 设为新的json 字符串
                String result = jsonObject.toJSONString();
                event.setBody(result.getBytes());
                LOG.info("modify event body success!!!!");
            } catch (Exception e) {
                LOG.error("deserializeFlumeEventBodyToJson exception", e);
            }
        } else {
            LOG.info("className {} 是空的！", className);
        }
    }

    public static String deserializeToTBaseJson(String s, String className) throws Exception {
        TDeserializer tde = new TDeserializer(new TBinaryProtocol.Factory());
        Base64 decoder = new Base64(0);
        TBase t = (TBase) Class.forName(className).newInstance();
        tde.deserialize(t, decoder.decodeBase64(s.getBytes()));
        return JSON.toJSONString(t);
    }
}
