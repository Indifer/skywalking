package org.apache.skywalking.oap.query.transaction;

import com.coxautodev.graphql.tools.SchemaParser;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.query.QueryModule;
import org.apache.skywalking.oap.server.core.server.JettyHandlerRegister;
import org.apache.skywalking.oap.server.library.module.*;

/**
 * @author yi.liang
 * @since JDK1.8
 * date 2019.07.19 16:01
 */
public class HealthQueryProvider  extends ModuleProvider {

    private final HealthQueryConfig config = new HealthQueryConfig();

//    private GraphQL graphQL;

    @Override public String name() {
        return "transaction";
    }

    @Override public Class<? extends ModuleDefine> module() {
        return QueryModule.class;
    }

    @Override public ModuleConfig createConfigBeanIfAbsent() {
        return config;
    }

    @Override public void prepare() throws ServiceNotProvidedException, ModuleStartException {
//        GraphQLSchema schema = SchemaParser.newParser()
//                .file("query-protocol/common.graphqls")
//                .resolvers(new Query(), new Mutation())
//                .file("query-protocol/metadata.graphqls")
//                .resolvers(new MetadataQuery(getManager()))
//                .file("query-protocol/metric.graphqls")
//                .resolvers(new MetricQuery(getManager()))
//                .file("query-protocol/topology.graphqls")
//                .resolvers(new TopologyQuery(getManager()))
//                .file("query-protocol/trace.graphqls")
//                .resolvers(new TraceQuery(getManager()))
//                .file("query-protocol/aggregation.graphqls")
//                .resolvers(new AggregationQuery(getManager()))
//                .file("query-protocol/alarm.graphqls")
//                .resolvers(new AlarmQuery(getManager()))
//                .file("query-protocol/top-n-records.graphqls")
//                .resolvers(new TopNRecordsQuery(getManager()))
//                .file("query-protocol/log.graphqls")
//                .resolvers(new LogQuery(getManager()))
//                .build()
//                .makeExecutableSchema();
//        this.graphQL = GraphQL.newGraphQL(schema).build();
    }

    @Override public void start() throws ServiceNotProvidedException, ModuleStartException {
        JettyHandlerRegister service = getManager().find(CoreModule.NAME).provider().getService(JettyHandlerRegister.class);
        service.addHandler(new HealthQueryHandler(config.getPath()));
    }

    @Override public void notifyAfterCompleted() throws ServiceNotProvidedException, ModuleStartException {

    }

    @Override public String[] requiredModules() {
        return new String[0];
    }
}
