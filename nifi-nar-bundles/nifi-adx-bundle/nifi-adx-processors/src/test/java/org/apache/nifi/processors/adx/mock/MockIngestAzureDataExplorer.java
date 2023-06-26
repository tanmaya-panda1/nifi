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
package org.apache.nifi.processors.adx.mock;

import com.microsoft.azure.kusto.ingest.IngestionProperties;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.adx.IngestAzureDataExplorer;

public class MockIngestAzureDataExplorer extends IngestAzureDataExplorer {

    @Override
    protected void checkIfStreamingPolicyIsEnabledInADX(String entityName, String database) {
    }

    protected boolean checkIfIngestorRoleDoesntExist(String databaseName, String tableName) {
        return false;
    }

    @Override
    protected String showOriginalTableRetentionPolicy(IngestionProperties ingestionProperties) {
        return "sampleRetentionPolicy";
    }

    @Override
    protected void dropTempTableIfExists(IngestionProperties ingestionPropertiesCreateTempTable) {
    }

    @Override
    protected void createTempTable(IngestionProperties ingestionPropertiesCreateTempTable, IngestionProperties ingestionProperties) throws ProcessException {
    }

    @Override
    protected void alterTempTableRetentionPolicy(IngestionProperties ingestionPropertiesCreateTempTable,ProcessContext context) {
    }

    @Override
    protected void alterTempTableAutoDeletePolicy(IngestionProperties ingestionPropertiesCreateTempTable, String expiryDate) {
    }

    @Override
    protected boolean isNifiClusteredSetup(NodeTypeProvider nodeTypeProvider){
        return nodeTypeProvider.isClustered();
    }

    @Override
    protected boolean shouldUseMaterializedViewFlag(String databaseName, String tableName) {
        return false;
    }

    @Override
    protected String executeMoveExtentsAsyncOperation(String databaseName, String moveExtentsQuery) {
        return "1234";
    }

    @Override
    protected String pollAndFindExtentMergeAsyncOperation(final String databaseName, final String showOperationsQuery, final String stateCol) {
        return "Completed";
    }

}