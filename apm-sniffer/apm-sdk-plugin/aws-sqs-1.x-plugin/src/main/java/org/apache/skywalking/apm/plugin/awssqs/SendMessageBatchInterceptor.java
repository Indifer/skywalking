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

package org.apache.skywalking.apm.plugin.awssqs;

import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageBatchResultEntry;
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
import java.net.URI;

public class SendMessageBatchInterceptor implements InstanceMethodsAroundInterceptor {

    public static final String OPERATE_NAME_PREFIX = "AWS-SQS/";
    public static final String PRODUCER_OPERATE_NAME_SUFFIX = "/Producer";

    @Override
    public void beforeMethod(EnhancedInstance objInst, Method method, Object[] allArguments,
                             Class<?>[] argumentsTypes, MethodInterceptResult result) throws Throwable {

        if (allArguments.length == 0 || allArguments[0] == null || !(allArguments[0] instanceof SendMessageBatchRequest)) {
            // illegal args, can't trace. ignore.
            return;
        }

        final SendMessageBatchRequest sendMessageRequest = (SendMessageBatchRequest) allArguments[0];
        final URI endpoint = (URI) objInst.getSkyWalkingDynamicField();
        String path = endpoint != null ? endpoint.getPath() : "";

        String queueUrl = sendMessageRequest.getQueueUrl();
        ContextCarrier contextCarrier = new ContextCarrier();
        AbstractSpan activeSpan = ContextManager.createExitSpan(OPERATE_NAME_PREFIX + queueUrl
                + PRODUCER_OPERATE_NAME_SUFFIX, contextCarrier, (String) path);

        Tags.MQ_QUEUE.set(activeSpan, queueUrl);
        activeSpan.setComponent(ComponentsDefine.AWS_SQS_PRODUCER);
        SpanLayer.asMQ(activeSpan);

        for (SendMessageBatchRequestEntry message : sendMessageRequest.getEntries()) {

            CarrierItem next = contextCarrier.items();
            while (next.hasNext()) {
                next = next.next();

                message.getMessageAttributes().put(next.getHeadKey(),
                        new MessageAttributeValue().withStringValue(next.getHeadValue()).withDataType("String"));
            }
        }
    }

    @Override
    public Object afterMethod(EnhancedInstance objInst, Method method, Object[] allArguments,
                              Class<?>[] argumentsTypes, Object ret) throws Throwable {

        AbstractSpan activeSpan = ContextManager.activeSpan();
        if (activeSpan != null && ret instanceof SendMessageBatchResult) {
            String[] msgIds = new String[((SendMessageBatchResult) ret).getSuccessful().size()];
            for (int i = 0; i < ((SendMessageBatchResult) ret).getSuccessful().size(); i++) {
                SendMessageBatchResultEntry sendMessageBatchResultEntry = ((SendMessageBatchResult) ret).getSuccessful().get(i);
                msgIds[i] = sendMessageBatchResultEntry.getMessageId();
            }

            Tags.MQ_MSG_ID.set(activeSpan, StringUtil.join(',', msgIds));
        }

        ContextManager.stopSpan();
        return ret;
    }

    @Override
    public void handleMethodException(EnhancedInstance objInst, Method method, Object[] allArguments,
                                      Class<?>[] argumentsTypes, Throwable t) {
        ContextManager.activeSpan().errorOccurred().log(t);

    }
}
