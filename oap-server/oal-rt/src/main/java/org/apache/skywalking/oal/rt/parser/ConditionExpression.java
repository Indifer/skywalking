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

package org.apache.skywalking.oal.rt.parser;

import java.util.LinkedList;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class ConditionExpression {
    // original from script
    private String expressionType;
    private String attribute;
    private String value;
    private List<String> values;

    public ConditionExpression(final String expressionType, final String attribute, final String value) {
        this.expressionType = expressionType;
        this.attribute = attribute;
        this.value = value;
    }

    public void addValue(String value) {
        if (values != null) {
            values.add(value);
        } else {
            this.value = value;
        }
    }

    public void enterMultiConditionValue() {
        values = new LinkedList<>();
    }

    public void exitMultiConditionValue() {
        value = "new Object[]{" + String.join(",", values) + "}";
    }
}
