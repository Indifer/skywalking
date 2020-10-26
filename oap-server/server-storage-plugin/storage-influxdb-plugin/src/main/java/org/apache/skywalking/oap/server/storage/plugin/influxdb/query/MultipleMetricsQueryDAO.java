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

package org.apache.skywalking.oap.server.storage.plugin.influxdb.query;

import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.apm.util.StringUtil;
import org.apache.skywalking.oap.server.core.analysis.manual.segment.SegmentRecord;
import org.apache.skywalking.oap.server.core.analysis.metrics.DataTable;
import org.apache.skywalking.oap.server.core.analysis.metrics.LongAvgMetrics;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;
import org.apache.skywalking.oap.server.core.analysis.metrics.PercentMetrics;
import org.apache.skywalking.oap.server.core.analysis.metrics.PercentileMetrics;
import org.apache.skywalking.oap.server.core.query.input.MultipleMetricsCondition;
import org.apache.skywalking.oap.server.core.query.type.MetricsValues;
import org.apache.skywalking.oap.server.core.query.type.MultipleMetrics;
import org.apache.skywalking.oap.server.core.storage.annotation.ValueColumnMetadata;
import org.apache.skywalking.oap.server.core.storage.query.IMultipleMetricsQueryDAO;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.apache.skywalking.oap.server.storage.plugin.influxdb.InfluxClient;
import org.apache.skywalking.oap.server.storage.plugin.influxdb.InfluxConstants;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.querybuilder.SelectQueryImpl;
import org.influxdb.querybuilder.WhereQueryImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.skywalking.oap.server.core.analysis.MetricsUtils.is;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.eq;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.select;

@Slf4j
public class MultipleMetricsQueryDAO implements IMultipleMetricsQueryDAO {

    private final InfluxClient client;

    public MultipleMetricsQueryDAO(InfluxClient client) {
        this.client = client;
    }

    @Override
    public List<MultipleMetrics> readMultipleMetrics(MultipleMetricsCondition condition, long timeBucket) throws IOException {

        String entityId = condition.getEntity().buildId();

        Map<String, MultipleMetrics> multipleMetricsMap = new HashMap<>();
        StringBuilder command = new StringBuilder();

        for (String metricsName : condition.getNames()) {

            WhereQueryImpl<SelectQueryImpl> query = select()
                    .from(client.getDatabase(), metricsName)
                    .where(eq(Metrics.TIME_BUCKET, timeBucket));

            if (!StringUtil.isEmpty(entityId)) {
                if (condition.getEntity().isService()) {
                    query.where(eq(Metrics.ENTITY_ID, entityId));
                } else {
                    query.where(eq(SegmentRecord.SERVICE_ID, entityId));
                }
            }

            command.append(query.getCommand());
        }

        Query query = new Query(command.toString());
        List<QueryResult.Result> results = client.query(query);

        for (QueryResult.Result result : results) {
            if (CollectionUtils.isNotEmpty(result.getSeries())) {

                for (QueryResult.Series series : result.getSeries()) {

                    MetricsValues metricsValues = new MetricsValues();
                    metricsValues.setLabel(series.getName());

                    Map<String, Integer> columnMap = new HashMap<>();
                    for (int i = 0; i < series.getColumns().size(); i++) {
                        columnMap.put(series.getColumns().get(i), i);
                    }

                    for (List<Object> values : series.getValues()) {

                        readResult(columnMap, values, metricsValues, series.getName());

                        int i = columnMap.get(InfluxConstants.ID_COLUMN);
                        String id = (String) values.get(i);

                        MultipleMetrics multipleMetrics = multipleMetricsMap.get(id);
                        if (multipleMetrics == null) {
                            multipleMetrics = new MultipleMetrics();
                            multipleMetrics.setTimeBucket(String.valueOf(timeBucket));
                            multipleMetrics.setId(id);
                            multipleMetrics.setEntityId(entityId);

                            multipleMetricsMap.put(id, multipleMetrics);
                        }
                        multipleMetrics.getMetrics().add(metricsValues);

                    }
                }

            }
        }

        return new ArrayList<>(multipleMetricsMap.values());
    }

    private void readResult(Map<String, Integer> columnMap, List<Object> values, MetricsValues metricsValues, String metricsName) {

        if (is(metricsName, PercentileMetrics.class)) {

            String percentileValues = (String) values.get(columnMap.get(PercentileMetrics.VALUE));
            DataTable dataTable = new DataTable(percentileValues);

            List<Integer> ranks = PercentileMetrics.RANKS;
            for (String key : dataTable.keys()) {
                int index = Integer.parseInt(key);
                if (index < ranks.size()) {
                    metricsValues.addIntValue(String.valueOf(ranks.get(index)), dataTable.get(key));
                }
            }

        } else if (is(metricsName, LongAvgMetrics.class)) {

            metricsValues.addIntValue(LongAvgMetrics.VALUE, (Integer) values.get(columnMap.get(LongAvgMetrics.VALUE)));
            metricsValues.addIntValue(LongAvgMetrics.VALUE, (Integer) values.get(columnMap.get(LongAvgMetrics.COUNT)));
            metricsValues.addIntValue(LongAvgMetrics.VALUE, (Integer) values.get(columnMap.get(LongAvgMetrics.MAX)));
            metricsValues.addIntValue(LongAvgMetrics.VALUE, (Integer) values.get(columnMap.get(LongAvgMetrics.MIN)));

        } else if (is(metricsName, PercentMetrics.class)) {

            metricsValues.addIntValue(PercentMetrics.PERCENTAGE, (Integer) values.get(columnMap.get(PercentMetrics.PERCENTAGE)));
            metricsValues.addIntValue(PercentMetrics.TOTAL, (Integer) values.get(columnMap.get(PercentMetrics.TOTAL)));
            metricsValues.addIntValue(PercentMetrics.MATCH, (Integer) values.get(columnMap.get(PercentMetrics.MATCH)));

        } else {
            String valueColumnName = ValueColumnMetadata.INSTANCE.getValueCName(metricsName);
            metricsValues.addIntValue(valueColumnName, (Integer) values.get(columnMap.get(valueColumnName)));
        }

    }
}
