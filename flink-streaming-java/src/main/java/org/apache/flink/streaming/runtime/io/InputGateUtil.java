/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;

import java.util.List;

/**
 * Utility for dealing with input gates. This will either just return the single {@link InputGate}
 * that was passed in or create a {@link UnionInputGate} if several {@link InputGate input gates}
 * are given.
 */
@Internal
public class InputGateUtil {

    public static InputGate createInputGate(List<IndexedInputGate> inputGates) {
        if (inputGates.size() <= 0) {
            throw new RuntimeException("No such input gate.");
        }
        // TODO 如果只有一个IndexedInputGate，就返回IndexedInputGate,实际是InputGateWithMetrics
        if (inputGates.size() == 1) {
            return inputGates.get(0);
        } else {
            // TODO 如果有多个就返回UnionInputGate，里面持有Map<Integer, InputGate>集合,实际是Map<Integer, InputGateWithMetrics>
            return new UnionInputGate(inputGates.toArray(new IndexedInputGate[0]));
        }
    }

    /** Private constructor to prevent instantiation. */
    private InputGateUtil() {
        throw new RuntimeException();
    }
}
