/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.skywalking.oap.server.core.analysis;

import lombok.SneakyThrows;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;

import java.util.HashMap;
import java.util.Map;

public class MetricsModelMapping {

    public static MetricsModelMapping INSTANCE = new MetricsModelMapping();
    private final Map<String, Class<? extends Metrics>> REGISTER = new HashMap<>();

    public void put(String metricsName, Class<? extends Metrics> metricsClass) {
        REGISTER.put(metricsName, metricsClass);
    }

    @SneakyThrows
    public Class<? extends Metrics> find(String metricsName) {

        Class<? extends Metrics> metricsClass = REGISTER.get(metricsName);
        if (metricsClass == null) {
            throw new IllegalArgumentException("Can't find metrics, " + metricsName);
        }
        return metricsClass;
    }
}
