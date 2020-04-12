/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.skywalking.apm.plugin.awssqs.xplat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xforceplus.xplat.aws.SqsData;
import com.xforceplus.xplat.aws.sqs.bean.MessageXplat;
import org.apache.skywalking.apm.agent.core.context.CarrierItem;
import org.apache.skywalking.apm.agent.core.context.ContextCarrier;
import org.apache.skywalking.apm.agent.core.context.ContextManager;
import org.apache.skywalking.apm.agent.core.context.tag.Tags;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.agent.core.context.trace.SpanLayer;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.EnhancedInstance;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceMethodsAroundInterceptor;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.MethodInterceptResult;
import org.apache.skywalking.apm.network.trace.component.ComponentsDefine;
import org.apache.skywalking.apm.util.StringUtil;

import java.lang.reflect.Method;
import java.util.HashMap;

public class DoListenerInterceptor implements InstanceMethodsAroundInterceptor {

    public static final String OPERATE_NAME_PREFIX = "AWS-SQS/";
    public static final String LISTENER_OPERATE_NAME_SUFFIX = "/Listener";

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public void beforeMethod(EnhancedInstance objInst, Method method, Object[] allArguments,
                             Class<?>[] argumentsTypes, MethodInterceptResult result) throws Throwable {

        if (allArguments.length == 0 || allArguments[0] == null || !(allArguments[0] instanceof MessageXplat)) {
            // illegal args, can't trace. ignore.
            return;
        }

        MessageXplat messageXplat = (MessageXplat) allArguments[0];
        SqsData sqsData = messageXplat.getSqsData();
        String queueUrl = sqsData.getQueueName();

        ContextCarrier contextCarrier = new ContextCarrier();
        AbstractSpan activeSpan = ContextManager.createEntrySpan(OPERATE_NAME_PREFIX +
            sqsData.getQueueName() + LISTENER_OPERATE_NAME_SUFFIX, null)
            .start(System.currentTimeMillis());

        Tags.MQ_QUEUE.set(activeSpan, queueUrl);
        Tags.MQ_MSG_ID.set(activeSpan, sqsData.getMessageId());
        activeSpan.setComponent(ComponentsDefine.AWS_SQS_CONSUMER);
        SpanLayer.asMQ(activeSpan);

        String props = sqsData.getProperties();
        if (!StringUtil.isEmpty(props)) {

            HashMap map = mapper.readValue(sqsData.getProperties(), HashMap.class);

            CarrierItem next = contextCarrier.items();
            while (next.hasNext()) {
                next = next.next();

                Object value = (String) map.get(next.getHeadKey());
                if (value != null) {
                    next.setHeadValue(value.toString());
                }
            }
        }

        ContextManager.extract(contextCarrier);
    }

    @Override
    public Object afterMethod(EnhancedInstance objInst, Method method, Object[] allArguments,
                              Class<?>[] argumentsTypes, Object ret) throws Throwable {

        ContextManager.stopSpan();
        return ret;
    }

    @Override
    public void handleMethodException(EnhancedInstance objInst, Method method, Object[] allArguments,
                                      Class<?>[] argumentsTypes, Throwable t) {
        ContextManager.activeSpan().errorOccurred().log(t);
    }
}
