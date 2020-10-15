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
 *
 */

package org.apache.skywalking.oap.server.core.query;

import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.query.input.MultipleMetricsCondition;
import org.apache.skywalking.oap.server.core.query.type.MultipleMetrics;
import org.apache.skywalking.oap.server.core.storage.StorageModule;
import org.apache.skywalking.oap.server.core.storage.model.IModelManager;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.query.IMultipleMetricsQueryDAO;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.apache.skywalking.oap.server.library.module.Service;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class MultipleMetricsQueryService implements Service {
    private final ModuleManager moduleManager;
    private IMultipleMetricsQueryDAO multipleMetricsQueryDAO;
    private IModelManager modelManager;

    public MultipleMetricsQueryService(ModuleManager moduleManager) {
        this.moduleManager = moduleManager;
    }

    public IMultipleMetricsQueryDAO getMultipleMetricsQueryDAO() {

        if (multipleMetricsQueryDAO == null) {
            multipleMetricsQueryDAO = moduleManager.find(StorageModule.NAME).provider().getService(IMultipleMetricsQueryDAO.class);
        }
        return multipleMetricsQueryDAO;
    }

    public IModelManager getModelManager() {
        if (modelManager == null) {
            modelManager = moduleManager.find(CoreModule.NAME).provider().getService(IModelManager.class);
        }
        return modelManager;
    }

    public List<MultipleMetrics> readMultipleMetrics(final MultipleMetricsCondition condition,
                                                     final String timeBucket) throws IOException {

        Set<String> metricsSet = new HashSet<>(condition.getNames());
        List<Model> models = getModelManager().allModels().stream()
                                                          .filter(model -> metricsSet.contains(model.getName()))
                                                          .collect(Collectors.toList());

        return getMultipleMetricsQueryDAO().readMultipleMetrics(condition, DurationUtils.INSTANCE.convertToTimeBucket(timeBucket));
    }

}
