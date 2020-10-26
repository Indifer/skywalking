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

package org.apache.skywalking.oap.server.storage.plugin.elasticsearch.query;

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
import org.apache.skywalking.oap.server.library.client.elasticsearch.ElasticSearchClient;
import org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base.EsDAO;
import org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base.MultipleIndexNameMaker;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.skywalking.apm.util.MapUtils.longValue;
import static org.apache.skywalking.oap.server.core.analysis.MetricsUtils.is;

public class MultipleMetricsQueryESDAO extends EsDAO implements IMultipleMetricsQueryDAO {

    public MultipleMetricsQueryESDAO(ElasticSearchClient client) {
        super(client);
    }

    @Override
    public List<MultipleMetrics> readMultipleMetrics(final MultipleMetricsCondition condition,
                                                     final long timeBucket) throws IOException {

        String entityId = condition.getEntity().buildId();

        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.termQuery(Metrics.TIME_BUCKET, timeBucket));

        if (!StringUtil.isEmpty(entityId)) {
            if (condition.getEntity().isService()) {

                boolQuery.must(QueryBuilders.termQuery(Metrics.ENTITY_ID, entityId));
            } else {

                boolQuery.must(QueryBuilders.termQuery(SegmentRecord.SERVICE_ID, entityId));
            }
        }

        sourceBuilder.query(boolQuery);

        Map<String, MultipleMetrics> multipleMetricsMap = new HashMap<>();
        String[] indexNames = condition.getNames().toArray(new String[0]);
        MultipleIndexNameMaker indexNameMaker = new MultipleIndexNameMaker(indexNames, timeBucket);
        Map<String, String> metricsMap = indexNameMaker.makeMap(getClient()::formatIndexName);

        SearchResponse response = getClient().search(indexNameMaker, sourceBuilder);

        for (SearchHit searchHit : response.getHits().getHits()) {
            Map<String, Object> source = searchHit.getSourceAsMap();
            String hitId = searchHit.getId();

            if (!StringUtil.isEmpty(hitId)) {

                MultipleMetrics multipleMetrics = multipleMetricsMap.get(hitId);
                if (multipleMetrics == null) {
                    multipleMetrics = new MultipleMetrics();
                    multipleMetrics.setTimeBucket(String.valueOf(timeBucket));
                    multipleMetrics.setId(hitId);
                    multipleMetrics.setEntityId(entityId);

                    multipleMetricsMap.put(hitId, multipleMetrics);
                }

                String metricsName = metricsMap.get(searchHit.getIndex());
                MetricsValues metricsValues = new MetricsValues();
                metricsValues.setLabel(metricsName);

                if (is(metricsName, PercentileMetrics.class)) {

                    String percentileValues = (String) source.get(PercentileMetrics.VALUE);
                    DataTable dataTable = new DataTable(percentileValues);

                    List<Integer> ranks = PercentileMetrics.RANKS;
                    for (String key : dataTable.keys()) {
                        int index = Integer.parseInt(key);
                        if (index < ranks.size()) {
                            metricsValues.addIntValue(String.valueOf(ranks.get(index)), dataTable.get(key));
                        }
                    }

                } else if (is(metricsName, LongAvgMetrics.class)) {

                    metricsValues.addIntValue(LongAvgMetrics.VALUE, longValue(source, LongAvgMetrics.VALUE));
                    metricsValues.addIntValue(LongAvgMetrics.COUNT, longValue(source, LongAvgMetrics.COUNT));
                    metricsValues.addIntValue(LongAvgMetrics.MAX, longValue(source, LongAvgMetrics.MAX));
                    metricsValues.addIntValue(LongAvgMetrics.MIN, longValue(source, LongAvgMetrics.MIN));

                } else if (is(metricsName, PercentMetrics.class)) {

                    metricsValues.addIntValue(PercentMetrics.MATCH, longValue(source, PercentMetrics.MATCH));
                    metricsValues.addIntValue(PercentMetrics.PERCENTAGE, longValue(source, PercentMetrics.PERCENTAGE));
                    metricsValues.addIntValue(PercentMetrics.TOTAL, longValue(source, PercentMetrics.TOTAL));

                } else {
                    String valueColumnName = ValueColumnMetadata.INSTANCE.getValueCName(metricsName);
                    metricsValues.addIntValue(valueColumnName, longValue(source, valueColumnName));
                }

                multipleMetrics.getMetrics().add(metricsValues);

            }
        }

        return new ArrayList<>(multipleMetricsMap.values());
    }
}
