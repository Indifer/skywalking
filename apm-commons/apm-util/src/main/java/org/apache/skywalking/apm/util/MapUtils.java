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

package org.apache.skywalking.apm.util;

import java.util.Map;

/**
 *
 */
public final class MapUtils {

    public static int intValue(Map map, Object key) {
        Number val = (Number) map.get(key);
        if (val == null) {
            return 0;
        }

        return val.intValue();
    }

    public static long longValue(Map map, Object key) {
        Number val = (Number) map.get(key);
        if (val == null) {
            return 0;
        }

        return val.longValue();
    }

    public static double doubleValue(Map map, Object key) {
        Number val = (Number) map.get(key);
        if (val == null) {
            return 0;
        }

        return val.doubleValue();
    }

    public static float floatValue(Map map, Object key) {
        Number val = (Number) map.get(key);
        if (val == null) {
            return 0;
        }

        return val.floatValue();
    }
}
