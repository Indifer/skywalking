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

package org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base;

import org.apache.skywalking.oap.server.core.analysis.DownSampling;
import org.apache.skywalking.oap.server.core.analysis.TimeBucket;
import org.apache.skywalking.oap.server.library.client.elasticsearch.IndexNameMaker;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * the multiple index maker works for super size dataset
 */
public class MultipleIndexNameMaker implements IndexNameMaker {

    private final String[] indexNames;
    private final DownSampling downSampling;
    private final long timeBucket;

    public MultipleIndexNameMaker(final String[] indexNames, final long timeBucket) {
        this.indexNames = indexNames;
        this.downSampling = TimeBucket.inferDownSampling(timeBucket);
        this.timeBucket = timeBucket;
    }

    @Override
    public String[] make() {
        return Arrays.stream(indexNames)
                .map(item -> TimeSeriesUtils.superDatasetIndexName(item, downSampling, timeBucket))
                .toArray(String[]::new);
    }

    public Map<String, String> makeMap(Function<String, String> handler) {
        return Arrays.stream(indexNames)
                .collect(Collectors.toMap(item -> handler.apply(TimeSeriesUtils.superDatasetIndexName(item, downSampling, timeBucket)), item -> item));
    }
}
