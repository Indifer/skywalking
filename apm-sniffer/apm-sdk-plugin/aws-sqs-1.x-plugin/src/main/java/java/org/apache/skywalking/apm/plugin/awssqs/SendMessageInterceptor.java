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

package java.org.apache.skywalking.apm.plugin.awssqs;

import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
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

import java.lang.reflect.Method;
import java.net.URI;

public class SendMessageInterceptor implements InstanceMethodsAroundInterceptor {

    public static final String OPERATE_NAME_PREFIX = "AWS-SQS/";
    public static final String PRODUCER_OPERATE_NAME_SUFFIX = "/Producer";

    @Override
    public void beforeMethod(EnhancedInstance objInst, Method method, Object[] allArguments,
                             Class<?>[] argumentsTypes, MethodInterceptResult result) throws Throwable {

        if (allArguments.length == 0 || allArguments[0] == null || !(allArguments[0] instanceof SendMessageRequest)) {
            // illegal args, can't trace. ignore.
            return;
        }
        final SendMessageRequest sendMessageRequest = (SendMessageRequest) allArguments[0];
        final URI endpoint = (URI) objInst.getSkyWalkingDynamicField();
        String url = endpoint != null ? endpoint.toString() : "";

        String queueUrl = sendMessageRequest.getQueueUrl();

        ContextCarrier contextCarrier = new ContextCarrier();
        AbstractSpan activeSpan = ContextManager.createExitSpan(OPERATE_NAME_PREFIX + queueUrl
                + PRODUCER_OPERATE_NAME_SUFFIX, contextCarrier, url);

        Tags.MQ_QUEUE.set(activeSpan, queueUrl);
        SpanLayer.asMQ(activeSpan);
        activeSpan.setComponent(ComponentsDefine.AWS_SQS_PRODUCER);

        CarrierItem next = contextCarrier.items();
        while (next.hasNext()) {
            next = next.next();
            sendMessageRequest.getMessageAttributes().put(next.getHeadKey(),
                    new MessageAttributeValue().withStringValue(next.getHeadValue()).withDataType("String"));
        }

    }

    @Override
    public Object afterMethod(EnhancedInstance objInst, Method method, Object[] allArguments,
                              Class<?>[] argumentsTypes, Object ret) throws Throwable {

        AbstractSpan activeSpan = ContextManager.activeSpan();
        if (activeSpan != null && ret instanceof SendMessageResult) {
            Tags.MQ_MSG_ID.set(activeSpan, ((SendMessageResult) ret).getMessageId());
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
