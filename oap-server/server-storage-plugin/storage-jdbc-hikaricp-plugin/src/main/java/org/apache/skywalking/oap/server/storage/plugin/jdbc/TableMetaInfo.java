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

package org.apache.skywalking.oap.server.storage.plugin.jdbc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.skywalking.oap.server.core.storage.model.ColumnName;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.model.ModelColumn;

@Getter
@Builder
@AllArgsConstructor
public class TableMetaInfo {
    private static final Map<String, TableMetaInfo> TABLES = new HashMap<>();

    private Model model;
    private Map<String, String> columnAndStorageMap = new HashMap<>();

    public static void addModel(Model model) {

        final List<ModelColumn> columns = model.getColumns();
        final Map<String, String> columnMap = new HashMap<>();
        columns.forEach(column -> {
            ColumnName columnName = column.getColumnName();
            columnMap.put(columnName.getName(), columnName.getStorageName());
        });

        TableMetaInfo info = TableMetaInfo.builder()
                .model(model)
                .columnAndStorageMap(columnMap)
                .build();
        TABLES.put(model.getName(), info);
    }

    public static Model get(String moduleName) {
        return TABLES.get(moduleName).getModel();
    }

    public static Map<String, String> getColumnAndStorageMap(String moduleName) {
        return TABLES.get(moduleName).columnAndStorageMap;
    }

}
