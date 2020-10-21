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

package org.apache.skywalking.oap.server.storage.plugin.jdbc.h2.dao;

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
import org.apache.skywalking.oap.server.library.client.jdbc.hikaricp.JDBCHikariCPClient;
import org.apache.skywalking.oap.server.storage.plugin.jdbc.TableMetaInfo;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.skywalking.oap.server.core.analysis.MetricsUtils.is;

public class H2MultipleMetricsQueryDAO extends H2SQLExecutor implements IMultipleMetricsQueryDAO {

    private JDBCHikariCPClient h2Client;

    public H2MultipleMetricsQueryDAO(JDBCHikariCPClient h2Client) {
        this.h2Client = h2Client;
    }

    @Override
    public List<MultipleMetrics> readMultipleMetrics(MultipleMetricsCondition condition, long timeBucket) throws IOException {

        String entityId = condition.getEntity().buildId();

        Map<String, MultipleMetrics> multipleMetricsMap = new HashMap<>();
        List<String> sqls = new ArrayList<>();

        for (String metricsName : condition.getNames()) {

            StringBuilder sql = new StringBuilder();

            sql.append("select * from " + metricsName);
            sql.append(" where " + Metrics.TIME_BUCKET + " = " + timeBucket);

            if (!StringUtil.isEmpty(entityId)) {
                if (condition.getEntity().isService()) {

                    sql.append(" and " + Metrics.ENTITY_ID + " = ?;");
                } else {
                    sql.append(" and " + SegmentRecord.SERVICE_ID + " = ?;");
                }

            }

            sqls.add(sql.toString());
        }

        try (Connection connection = h2Client.getConnection()) {

            for (int i = 0; i < sqls.size(); i++) {
                String sql = sqls.get(i);

                try (ResultSet resultSet = h2Client.executeQuery(
                        connection, sql, entityId)) {
                    while (resultSet.next()) {

                        String metricsName = condition.getNames().get(i);
                        MetricsValues metricsValues = new MetricsValues();
                        metricsValues.setLabel(metricsName);

                        readResultSet(resultSet, metricsValues, metricsName);

                        String id = resultSet.getString(1);

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
                } catch (SQLException e) {
                    throw new IOException(e);
                }

            }

        } catch (SQLException e) {
            throw new IOException(e);
        }

        return new ArrayList<>(multipleMetricsMap.values());

    }

    private void readResultSet(ResultSet resultSet, MetricsValues metricsValues, String metricsName) throws SQLException {

        Map<String, String> columnAndStorageMap = TableMetaInfo.getColumnAndStorageMap(metricsName);

        if (is(metricsName, PercentileMetrics.class)) {

            String percentileValues = resultSet.getString(columnAndStorageMap.get(PercentileMetrics.VALUE));
            DataTable dataTable = new DataTable(percentileValues);

            int[] ranks = PercentileMetrics.ranksClone();
            for (String key : dataTable.keys()) {
                int index = Integer.parseInt(key);
                if (index < ranks.length) {
                    metricsValues.addIntValue(String.valueOf(ranks[index]), dataTable.get(key));
                }
            }

        } else if (is(metricsName, LongAvgMetrics.class)) {

            metricsValues.addIntValue(LongAvgMetrics.VALUE, resultSet.getLong(columnAndStorageMap.get(LongAvgMetrics.VALUE)));
            metricsValues.addIntValue(LongAvgMetrics.COUNT, resultSet.getLong(columnAndStorageMap.get(LongAvgMetrics.COUNT)));
            metricsValues.addIntValue(LongAvgMetrics.MAX, resultSet.getLong(columnAndStorageMap.get(LongAvgMetrics.MAX)));
            metricsValues.addIntValue(LongAvgMetrics.MIN, resultSet.getLong(columnAndStorageMap.get(LongAvgMetrics.MIN)));

        } else if (is(metricsName, PercentMetrics.class)) {

            metricsValues.addIntValue(PercentMetrics.PERCENTAGE, resultSet.getLong(columnAndStorageMap.get(PercentMetrics.PERCENTAGE)));
            metricsValues.addIntValue(PercentMetrics.TOTAL, resultSet.getLong(columnAndStorageMap.get(PercentMetrics.TOTAL)));
            metricsValues.addIntValue(PercentMetrics.MATCH, resultSet.getLong(columnAndStorageMap.get(PercentMetrics.MATCH)));

        } else {
            String valueColumnName = ValueColumnMetadata.INSTANCE.getValueCName(metricsName);
            metricsValues.addIntValue(valueColumnName, resultSet.getLong(valueColumnName));
        }
    }

}
