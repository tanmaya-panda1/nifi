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
package org.apache.nifi.processors.adx;

import org.apache.nifi.adx.service.StandardKustoIngestService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

class IngestAzureDataExplorerE2ETest {

    private org.apache.nifi.processors.adx.IngestAzureDataExplorer ingestAzureDataExplorer;

    private StandardKustoIngestService kustoIngestService;

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        ingestAzureDataExplorer = new org.apache.nifi.processors.adx.IngestAzureDataExplorer();
    }

    @Test
    void testAzureAdxIngestProcessorQueuedIngestionSingleNodeNonTransactionalE2E() throws InitializationException, IOException {

        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner = TestRunners.newTestRunner(ingestAzureDataExplorer);

        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.TABLE_NAME,System.getProperty("tableName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.MAPPING_NAME,System.getProperty("mappingName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.ADX_SERVICE,"adx-connection-service");

        testRunner.setValidateExpressionUsage(false);

        kustoIngestService = new StandardKustoIngestService();

        testRunner.addControllerService("adx-connection-service", kustoIngestService, new HashMap<>());

        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.INGEST_URL,System.getProperty("ingestUrl"));
        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.APP_ID,System.getProperty("appId"));
        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.APP_KEY,System.getProperty("appKey"));
        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.APP_TENANT,System.getProperty("appTenant"));
        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.CLUSTER_URL, System.getProperty("clusterUrl"));

        testRunner.enableControllerService(kustoIngestService);
        testRunner.assertValid(kustoIngestService);
        InputStream inputStream = this.getClass().getResourceAsStream("/fileQueuedSuccess.csv");
        testRunner.enqueue(inputStream);
        assert inputStream != null;
        inputStream.close();
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.IngestAzureDataExplorer.RL_SUCCEEDED);

    }

    @Test
    void testAzureAdxIngestProcessorQueuedIngestionSingleNodeTransactionalE2E() throws InitializationException, IOException {

        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner = TestRunners.newTestRunner(ingestAzureDataExplorer);

        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.TABLE_NAME,System.getProperty("tableName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.MAPPING_NAME,System.getProperty("mappingName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.ADX_SERVICE,"adx-connection-service");
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.IS_TRANSACTIONAL, org.apache.nifi.processors.adx.IngestAzureDataExplorer.TRANSACTIONAL_YES.getValue());

        testRunner.setValidateExpressionUsage(false);

        kustoIngestService = new StandardKustoIngestService();

        testRunner.addControllerService("adx-connection-service", kustoIngestService, new HashMap<>());

        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.INGEST_URL,System.getProperty("ingestUrl"));
        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.APP_ID,System.getProperty("appId"));
        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.APP_KEY,System.getProperty("appKey"));
        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.APP_TENANT,System.getProperty("appTenant"));
        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.CLUSTER_URL, System.getProperty("clusterUrl"));

        testRunner.enableControllerService(kustoIngestService);
        testRunner.assertValid(kustoIngestService);
        InputStream inputStream = this.getClass().getResourceAsStream("/fileQueuedSuccess.csv");
        testRunner.enqueue(inputStream);
        assert inputStream != null;
        inputStream.close();
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.IngestAzureDataExplorer.RL_SUCCEEDED);

    }

    @Test
    void testAzureAdxIngestProcessorQueuedIngestionSingleNodeTransactionalFailureE2E() throws InitializationException, IOException {

        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner = TestRunners.newTestRunner(ingestAzureDataExplorer);

        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.TABLE_NAME,System.getProperty("tableName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.MAPPING_NAME,System.getProperty("mappingName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.ADX_SERVICE,"adx-connection-service");
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.IS_TRANSACTIONAL, org.apache.nifi.processors.adx.IngestAzureDataExplorer.TRANSACTIONAL_YES.getValue());

        testRunner.setValidateExpressionUsage(false);

        kustoIngestService = new StandardKustoIngestService();

        testRunner.addControllerService("adx-connection-service", kustoIngestService, new HashMap<>());

        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.INGEST_URL,System.getProperty("ingestUrl"));
        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.APP_ID,System.getProperty("appId"));
        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.APP_KEY,System.getProperty("appKey"));
        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.APP_TENANT,System.getProperty("appTenant"));
        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.CLUSTER_URL, System.getProperty("clusterUrl"));

        testRunner.enableControllerService(kustoIngestService);
        testRunner.assertValid(kustoIngestService);
        InputStream inputStream = this.getClass().getResourceAsStream("/fileFailure.csv");
        testRunner.enqueue(inputStream);
        assert inputStream != null;
        inputStream.close();
        Assertions.assertThrows(AssertionError.class, () -> testRunner.run(1));
        //testRunner.assertQueueEmpty();
        //testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.IngestAzureDataExplorer.RL_FAILED);

    }

    @Test
    void testAzureAdxIngestProcessorStreamingIngestionE2E() throws InitializationException, IOException {

        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner = TestRunners.newTestRunner(ingestAzureDataExplorer);

        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.TABLE_NAME,System.getProperty("tableName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.MAPPING_NAME,System.getProperty("mappingName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.ADX_SERVICE,"adx-connection-service");
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.IS_STREAMING_ENABLED, "true");

        testRunner.setValidateExpressionUsage(false);

        kustoIngestService = new StandardKustoIngestService();

        testRunner.addControllerService("adx-connection-service", kustoIngestService, new HashMap<>());

        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.INGEST_URL,System.getProperty("ingestUrl"));
        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.APP_ID,System.getProperty("appId"));
        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.APP_KEY,System.getProperty("appKey"));
        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.APP_TENANT,System.getProperty("appTenant"));
        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.CLUSTER_URL, System.getProperty("clusterUrl"));

        testRunner.enableControllerService(kustoIngestService);
        testRunner.assertValid(kustoIngestService);
        InputStream inputStream = this.getClass().getResourceAsStream("/fileStreaming.csv");
        testRunner.enqueue(inputStream);
        assert inputStream != null;
        inputStream.close();
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.IngestAzureDataExplorer.RL_SUCCEEDED);
    }

    @Test
    void testAzureAdxIngestProcessorStreamingIngestionFailureE2E() throws InitializationException, IOException {

        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner = TestRunners.newTestRunner(ingestAzureDataExplorer);

        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.TABLE_NAME,System.getProperty("tableName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.MAPPING_NAME,System.getProperty("mappingName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.ADX_SERVICE,"adx-connection-service");
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.IS_STREAMING_ENABLED, "true");

        testRunner.setValidateExpressionUsage(false);

        kustoIngestService = new StandardKustoIngestService();

        testRunner.addControllerService("adx-connection-service", kustoIngestService, new HashMap<>());

        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.INGEST_URL,System.getProperty("ingestUrl"));
        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.APP_ID,System.getProperty("appId"));
        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.APP_KEY,System.getProperty("appKey"));
        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.APP_TENANT,System.getProperty("appTenant"));
        testRunner.setProperty(kustoIngestService, StandardKustoIngestService.CLUSTER_URL, System.getProperty("clusterUrl"));

        testRunner.enableControllerService(kustoIngestService);
        testRunner.assertValid(kustoIngestService);
        InputStream inputStream = this.getClass().getResourceAsStream("/fileFailure.csv");
        testRunner.enqueue(inputStream);
        assert inputStream != null;
        inputStream.close();
        Assertions.assertThrows(AssertionError.class, () -> testRunner.run(1));
        //testRunner.assertQueueEmpty();
        //testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.IngestAzureDataExplorer.RL_FAILED);

    }


}