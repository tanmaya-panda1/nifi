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
 */

package org.apache.nifi.kafka.connect;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StatelessNiFiSourceTaskIT {

    @Test
    public void testSimpleFlow(TestInfo testInfo) throws InterruptedException {
        final StatelessNiFiSourceTask sourceTask = new StatelessNiFiSourceTask();
        sourceTask.initialize(createContext());

        final Map<String, String> properties = createDefaultProperties(testInfo);
        sourceTask.start(properties);

        final List<SourceRecord> sourceRecords = sourceTask.poll();
        assertEquals(1, sourceRecords.size());

        final SourceRecord record = sourceRecords.get(0);
        assertEquals("Hello World", new String((byte[]) record.value()));
        assertNull(record.key());
        assertEquals("my-topic", record.topic());

        sourceTask.stop();
    }

    @Test
    public void testKeyAttribute(TestInfo testInfo) throws InterruptedException {
        final StatelessNiFiSourceTask sourceTask = new StatelessNiFiSourceTask();
        sourceTask.initialize(createContext());

        final Map<String, String> properties = createDefaultProperties(testInfo);
        properties.put(StatelessNiFiSourceConfig.KEY_ATTRIBUTE, "greeting");
        sourceTask.start(properties);

        final List<SourceRecord> sourceRecords = sourceTask.poll();
        assertEquals(1, sourceRecords.size());

        final SourceRecord record = sourceRecords.get(0);
        final Object key = record.key();
        assertEquals("hello", key);
        assertEquals("my-topic", record.topic());

        sourceTask.stop();
    }

    @Test
    public void testTopicNameAttribute(TestInfo testInfo) throws InterruptedException {
        final StatelessNiFiSourceTask sourceTask = new StatelessNiFiSourceTask();
        sourceTask.initialize(createContext());

        final Map<String, String> properties = createDefaultProperties(testInfo);
        properties.put(StatelessNiFiSourceConfig.TOPIC_NAME_ATTRIBUTE, "greeting");
        sourceTask.start(properties);

        final List<SourceRecord> sourceRecords = sourceTask.poll();
        assertEquals(1, sourceRecords.size());

        final SourceRecord record = sourceRecords.get(0);
        assertEquals("hello", record.topic());

        sourceTask.stop();
    }

    @Test
    public void testHeaders(TestInfo testInfo) throws InterruptedException {
        final StatelessNiFiSourceTask sourceTask = new StatelessNiFiSourceTask();
        sourceTask.initialize(createContext());

        final Map<String, String> properties = createDefaultProperties(testInfo);
        properties.put(StatelessNiFiSourceConfig.HEADER_REGEX, "uuid|greeting|num.*");
        sourceTask.start(properties);

        final List<SourceRecord> sourceRecords = sourceTask.poll();
        assertEquals(1, sourceRecords.size());

        final SourceRecord record = sourceRecords.get(0);
        assertEquals("my-topic", record.topic());

        final Map<String, String> headerValues = new HashMap<>();
        final Headers headers = record.headers();
        for (final Header header : headers) {
            headerValues.put(header.key(), (String) header.value());
        }

        assertEquals("hello", headerValues.get("greeting"));
        assertTrue(headerValues.containsKey("uuid"));
        assertTrue(headerValues.containsKey("number"));

        sourceTask.stop();
    }

    @Test
    public void testTransferToWrongPort(TestInfo testInfo) {
        final StatelessNiFiSourceTask sourceTask = new StatelessNiFiSourceTask();
        sourceTask.initialize(createContext());

        final Map<String, String> properties = createDefaultProperties(testInfo);
        properties.put(StatelessNiFiSourceConfig.OUTPUT_PORT_NAME, "Another");
        sourceTask.start(properties);

        assertThrows(RetriableException.class, () -> sourceTask.poll(), "Expected RetriableException to be thrown");
    }

    @Test
    public void testStateRecovered(TestInfo testInfo) {
        final OffsetStorageReader offsetStorageReader = new OffsetStorageReader() {
            @Override
            public <T> Map<String, Object> offset(final Map<String, T> partition) {
                if ("CLUSTER".equals(partition.get(StatelessNiFiSourceConfig.STATE_MAP_KEY))) {
                    final String serializedStateMap = "{\"version\":4,\"stateValues\":{\"abc\":\"123\"}}";
                    return Collections.singletonMap("c6562d38-4994-3fcc-ac98-1da34de1916f", serializedStateMap);
                }

                return null;
            }

            @Override
            public <T> Map<Map<String, T>, Map<String, Object>> offsets(final Collection<Map<String, T>> partitions) {
                return Collections.emptyMap();
            }
        };

        final StatelessNiFiSourceTask sourceTask = new StatelessNiFiSourceTask();
        sourceTask.initialize(createContext(offsetStorageReader));

        final Map<String, String> properties = createDefaultProperties(testInfo);
        properties.put(StatelessNiFiSourceConfig.OUTPUT_PORT_NAME, "Another");
        sourceTask.start(properties);

        final StatelessDataflow dataflow = sourceTask.getDataflow();
        final Map<String, String> localStates = dataflow.getComponentStates(Scope.LOCAL);
        final Map<String, String> clusterStates = dataflow.getComponentStates(Scope.CLUSTER);

        assertFalse(clusterStates.isEmpty());
        assertTrue(localStates.isEmpty());
    }

    @Test
    public void testStateProvidedAndRecovered(TestInfo testInfo) throws InterruptedException {
        final StatelessNiFiSourceTask sourceTask = new StatelessNiFiSourceTask();
        sourceTask.initialize(createContext());

        final Map<String, String> properties = createDefaultProperties(testInfo);
        sourceTask.start(properties);

        final List<SourceRecord> sourceRecords = sourceTask.poll();
        assertEquals(1, sourceRecords.size());

        final SourceRecord record = sourceRecords.get(0);
        assertEquals("Hello World", new String((byte[]) record.value()));
        assertNull(record.key());
        assertEquals("my-topic", record.topic());

        final Map<String, ?> sourceOffset = record.sourceOffset();
        assertNotNull(sourceOffset);
        assertEquals(1, sourceOffset.size());
        final String generateProcessorId = sourceOffset.keySet().iterator().next();

        final String serializedStateMap = "{\"version\":\"1\",\"stateValues\":{\"count\":\"1\"}}";
        final Map<String, ?> expectedSourceOffset = Collections.singletonMap(generateProcessorId, serializedStateMap);
        assertEquals(expectedSourceOffset, sourceOffset);

        final Map<String, ?> sourcePartition = record.sourcePartition();
        final Map<String, ?> expectedSourcePartition = Collections.singletonMap("task.index", "1");
        assertEquals(expectedSourcePartition, sourcePartition);

        sourceTask.stop();


        final OffsetStorageReader offsetStorageReader = new OffsetStorageReader() {
            @Override
            public <T> Map<String, Object> offset(final Map<String, T> partition) {
                if (sourcePartition.equals(partition)) {
                    return Collections.singletonMap(generateProcessorId, serializedStateMap);
                }

                return null;
            }

            @Override
            public <T> Map<Map<String, T>, Map<String, Object>> offsets(final Collection<Map<String, T>> partitions) {
                return Collections.emptyMap();
            }
        };

        sourceTask.initialize(createContext(offsetStorageReader));
        sourceTask.start(properties);

        final StatelessDataflow dataflow = sourceTask.getDataflow();
        final Map<String, String> localStates = dataflow.getComponentStates(Scope.LOCAL);
        final Map<String, String> clusterStates = dataflow.getComponentStates(Scope.CLUSTER);

        assertTrue(clusterStates.isEmpty());
        assertFalse(localStates.isEmpty());

        final String generateProcessorState = localStates.get(generateProcessorId);
        assertEquals(serializedStateMap, generateProcessorState);
    }


    private Map<String, String> createDefaultProperties(TestInfo testInfo) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(StatelessNiFiCommonConfig.DATAFLOW_TIMEOUT, "30 sec");
        properties.put(StatelessNiFiSourceConfig.OUTPUT_PORT_NAME, "Out");
        properties.put(StatelessNiFiSourceConfig.TOPIC_NAME, "my-topic");
        properties.put(StatelessNiFiSourceConfig.KEY_ATTRIBUTE, "kafka.key");
        properties.put(StatelessNiFiCommonConfig.FLOW_SNAPSHOT, "src/test/resources/flows/Generate_Data.json");
        properties.put(StatelessNiFiCommonConfig.NAR_DIRECTORY, "target/nifi-kafka-connector-bin/nars");
        properties.put(StatelessNiFiCommonConfig.WORKING_DIRECTORY, "target/nifi-kafka-connector-bin/working");
        properties.put(StatelessNiFiCommonConfig.DATAFLOW_NAME, testInfo.getTestMethod().get().getName());
        properties.put(StatelessNiFiSourceConfig.STATE_MAP_KEY, "1");

        return properties;
    }


    private SourceTaskContext createContext() {
        final OffsetStorageReader offsetStorageReader = createOffsetStorageReader();
        return createContext(offsetStorageReader);
    }

    private SourceTaskContext createContext(final OffsetStorageReader offsetStorageReader) {
        return new SourceTaskContext() {
            @Override
            public Map<String, String> configs() {
                return Collections.emptyMap();
            }

            @Override
            public OffsetStorageReader offsetStorageReader() {
                return offsetStorageReader;
            }
        };
    }

    private OffsetStorageReader createOffsetStorageReader() {
        return new OffsetStorageReader() {
            @Override
            public <T> Map<String, Object> offset(final Map<String, T> partition) {
                return Collections.emptyMap();
            }

            @Override
            public <T> Map<Map<String, T>, Map<String, Object>> offsets(final Collection<Map<String, T>> partitions) {
                return Collections.emptyMap();
            }
        };
    }
}
