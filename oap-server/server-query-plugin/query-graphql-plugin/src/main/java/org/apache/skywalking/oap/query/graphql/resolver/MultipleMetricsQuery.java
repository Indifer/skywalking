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

package org.apache.skywalking.oap.query.graphql.resolver;

import com.coxautodev.graphql.tools.GraphQLQueryResolver;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.query.DurationUtils;
import org.apache.skywalking.oap.server.core.query.MultipleMetricsQueryService;
import org.apache.skywalking.oap.server.core.query.input.MultipleMetricsCondition;
import org.apache.skywalking.oap.server.core.query.type.MultipleMetrics;
import org.apache.skywalking.oap.server.library.module.ModuleManager;

import java.io.IOException;
import java.util.List;

/**
 * @since 8.0.0
 */
public class MultipleMetricsQuery implements GraphQLQueryResolver {
    private final ModuleManager moduleManager;
    private MultipleMetricsQueryService multipleMetricsQueryService;

    public MultipleMetricsQuery(ModuleManager moduleManager) {
        this.moduleManager = moduleManager;
    }

    private MultipleMetricsQueryService getMultipleMetricsQueryService() {
        if (multipleMetricsQueryService == null) {
            this.multipleMetricsQueryService = moduleManager.find(CoreModule.NAME)
                                                            .provider()
                                                            .getService(MultipleMetricsQueryService.class);
        }
        return multipleMetricsQueryService;
    }

    /**
     * Read multiple metrics on the time-sharing
     */
    public List<MultipleMetrics> readMultipleMetrics(final MultipleMetricsCondition condition,
                                                     final long timeBucket) throws IOException {

        return getMultipleMetricsQueryService().readMultipleMetrics(condition, timeBucket);

    }
}
