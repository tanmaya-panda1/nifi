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
package org.apache.nifi.adx.mock;

import com.microsoft.azure.kusto.data.StreamingClient;
import org.apache.nifi.adx.service.StandardKustoQueryService;
import org.mockito.Mockito;

public class MockStandardKustoQueryService extends StandardKustoQueryService {

    @Override
    public StreamingClient createKustoExecutionClient(final String clusterUrl, final String appId, final String appKey, final String appTenant, final String kustoAuthStrategy) {
        return Mockito.mock(StreamingClient.class);
    }
}
