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
import org.apache.skywalking.oap.server.core.analysis.MetricsModelMapping;
import org.apache.skywalking.oap.server.core.analysis.manual.segment.SegmentRecord;
import org.apache.skywalking.oap.server.core.analysis.metrics.*;
import org.apache.skywalking.oap.server.core.query.input.MultipleMetricsCondition;
import org.apache.skywalking.oap.server.core.query.type.KVInt;
import org.apache.skywalking.oap.server.core.query.type.MultipleMetrics;
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
import static org.apache.skywalking.oap.server.core.analysis.MetricsUtils.getFunctionName;
import static org.apache.skywalking.oap.server.core.analysis.MetricsUtils.is;

public class MultipleMetricsQueryESDAO extends EsDAO implements IMultipleMetricsQueryDAO {

    public MultipleMetricsQueryESDAO(ElasticSearchClient client) {
        super(client);
    }

    @Override
    public List<MultipleMetrics> readMultipleMetrics(final MultipleMetricsCondition condition,
                                                     final long timeBucket) throws IOException {

        String entityId = condition.getEntity().buildId();
//        String id = StringUtil.isNotEmpty(entityId) ? new PointOfTime(timeBucket).id(entityId) : null;

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
                if (is(metricsName, PercentileMetrics.class)) {

                    String percentileValues = (String) source.get(PercentileMetrics.VALUE);
                    DataTable dataTable = new DataTable(percentileValues);

                    int[] ranks = PercentileMetrics.ranksClone();
                    for (String key : dataTable.keys()) {
                        int index = Integer.parseInt(key);
                        if (index < ranks.length) {
                            multipleMetrics.getMetrics().add(new KVInt(getFunctionName(PercentileMetrics.class) + ranks[index], dataTable.get(key)));
                        }
                    }

                } else if (is(metricsName, LongAvgMetrics.class)) {

                    multipleMetrics.getMetrics().add(new KVInt(LongAvgMetrics.COUNT, longValue(source, LongAvgMetrics.COUNT)));
                    multipleMetrics.getMetrics().add(new KVInt(LongAvgMetrics.MAX, longValue(source, LongAvgMetrics.MAX)));
                    multipleMetrics.getMetrics().add(new KVInt(LongAvgMetrics.MIN, longValue(source, LongAvgMetrics.MIN)));

                } else if (is(metricsName, CPMMetrics.class)) {

                    multipleMetrics.getMetrics().add(new KVInt(getFunctionName(CPMMetrics.class), longValue(source, CPMMetrics.VALUE)));

                } else if (is(metricsName, PercentMetrics.class)) {

                    multipleMetrics.getMetrics().add(new KVInt(PercentMetrics.MATCH, longValue(source, PercentMetrics.MATCH)));
                    multipleMetrics.getMetrics().add(new KVInt(PercentMetrics.PERCENTAGE, longValue(source, PercentMetrics.PERCENTAGE)));
                    multipleMetrics.getMetrics().add(new KVInt(PercentMetrics.TOTAL, longValue(source, PercentMetrics.TOTAL)));
                    multipleMetrics.getMetrics().add(new KVInt(PercentMetrics.MATCH, longValue(source, PercentMetrics.MATCH)));

                }


            }
        }

        return new ArrayList<>(multipleMetricsMap.values());
    }
}
