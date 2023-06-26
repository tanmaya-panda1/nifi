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


import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import org.apache.nifi.adx.KustoIngestService;
import org.apache.nifi.adx.service.StandardKustoIngestService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.adx.mock.MockIngestAzureDataExplorer;
import org.apache.nifi.processors.adx.mock.MockStandardKustoIngestionService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opentest4j.AssertionFailedError;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IngestAzureDataExplorerTest {

    private IngestAzureDataExplorer ingestAzureDataExplorer;
    private SharedSessionState sharedState;
    private static final String MOCK_DB_NAME= "mockDBName";
    private static final String MOCK_TABLE_NAME= "mockTableName";
    private static final String MOCK_MAPPING_NAME= "mockMappingName";
    private StandardKustoIngestService kustoIngestService;
    private TestRunner testRunner;


    @BeforeEach
    public void setup() throws InitializationException {
        ingestAzureDataExplorer = new MockIngestAzureDataExplorer();
        testRunner = TestRunners.newTestRunner(ingestAzureDataExplorer);
        kustoIngestService = new MockStandardKustoIngestionService();
        testRunner.addControllerService("adx-connection-service", kustoIngestService);
    }

    @Test
    void testAzureAdxIngestProcessorQueuedIngestionNonTransactional() throws IOException, URISyntaxException, IngestionClientException, IngestionServiceException {

        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.TABLE_NAME,MOCK_TABLE_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.MAPPING_NAME,MOCK_MAPPING_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.ADX_SERVICE,"adx-connection-service");

        testRunner.setValidateExpressionUsage(false);

        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.INGEST_URL,"http://ingest-sample.com/");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.APP_ID,"sample");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.APP_KEY,"sample");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.APP_TENANT,"sample");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.CLUSTER_URL, "http://sample.com/");

        testRunner.enableControllerService(kustoIngestService);
        testRunner.assertValid(kustoIngestService);
        InputStream inputStream = this.getClass().getResourceAsStream("/fileQueuedSuccess.csv");
        testRunner.enqueue(inputStream);
        assert inputStream != null;
        inputStream.close();

        IngestClient ingestClient = kustoIngestService.getIngestClient();
        Assertions.assertNotNull(ingestClient);

        IngestionResult ingestionResult = Mockito.mock(IngestionResult.class);
        IngestionStatus ingestionStatus = new IngestionStatus();
        ingestionStatus.setStatus(OperationStatus.Succeeded);
        Mockito.when(ingestionResult.getIngestionStatusCollection()).thenReturn(List.of(ingestionStatus));
        Mockito.when(ingestClient.ingestFromStream( Mockito.any(), Mockito.any())).thenReturn(ingestionResult);
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(org.apache.nifi.processors.adx.IngestAzureDataExplorer.RL_SUCCEEDED);
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.IngestAzureDataExplorer.RL_SUCCEEDED);

    }

    @Test
    void testAzureAdxIngestProcessorQueuedIngestionTransactional() throws IOException, URISyntaxException, IngestionClientException, IngestionServiceException {

        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.TABLE_NAME,MOCK_TABLE_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.MAPPING_NAME,MOCK_MAPPING_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.ADX_SERVICE,"adx-connection-service");
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.IS_TRANSACTIONAL, org.apache.nifi.processors.adx.IngestAzureDataExplorer.TRANSACTIONAL_YES);

        testRunner.setValidateExpressionUsage(false);

        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.INGEST_URL,"http://ingest-sample.com/");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.APP_ID,"sample");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.APP_KEY,"sample");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.APP_TENANT,"sample");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.CLUSTER_URL, "http://sample.com/");
        testRunner.enableControllerService(kustoIngestService);
        testRunner.assertValid(kustoIngestService);
        InputStream inputStream = this.getClass().getResourceAsStream("/fileQueuedSuccess.csv");
        testRunner.enqueue(inputStream);
        assert inputStream != null;
        inputStream.close();

        IngestClient ingestClient = kustoIngestService.getIngestClient();
        Assertions.assertNotNull(ingestClient);

        IngestionResult ingestionResult = Mockito.mock(IngestionResult.class);
        IngestionStatus ingestionStatus = new IngestionStatus();
        ingestionStatus.setStatus(OperationStatus.Succeeded);
        Mockito.when(ingestionResult.getIngestionStatusCollection()).thenReturn(List.of(ingestionStatus));
        Mockito.when(ingestClient.ingestFromStream( Mockito.any(), Mockito.any())).thenReturn(ingestionResult);

        testRunner.run(1);
        testRunner.assertQueueEmpty();
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(org.apache.nifi.processors.adx.IngestAzureDataExplorer.RL_SUCCEEDED);
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.IngestAzureDataExplorer.RL_SUCCEEDED);

    }

    @Test
    void testAzureAdxIngestProcessorManagedStreaming() throws InitializationException {

        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.TABLE_NAME,MOCK_TABLE_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.MAPPING_NAME,MOCK_MAPPING_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.ADX_SERVICE,"adx-connection-service");
        testRunner.setProperty(IngestAzureDataExplorer.IS_STREAMING_ENABLED,"true");

        testRunner.setValidateExpressionUsage(false);

        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.INGEST_URL,"https://ingest-sample.com/");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.APP_ID,"sample");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.APP_KEY,"sample");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.APP_TENANT,"sample");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.CLUSTER_URL,"https://sample-cluster.com/");

        testRunner.enableControllerService(kustoIngestService);

        testRunner.assertValid(kustoIngestService);

        testRunner.run(1);
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.IngestAzureDataExplorer.RL_SUCCEEDED);

    }

    @Test
    void testGetPropertyDescriptors() {

        testRunner = TestRunners.newTestRunner(ingestAzureDataExplorer);

        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.TABLE_NAME,MOCK_TABLE_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.MAPPING_NAME,MOCK_MAPPING_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.ADX_SERVICE,"adx-connection-service");


        List<PropertyDescriptor> pd = ingestAzureDataExplorer.getSupportedPropertyDescriptors();
        assertTrue(pd.contains(org.apache.nifi.processors.adx.IngestAzureDataExplorer.ADX_SERVICE));
        assertTrue(pd.contains(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DATA_FORMAT));
        assertTrue(pd.contains(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DB_NAME));
        assertTrue(pd.contains(org.apache.nifi.processors.adx.IngestAzureDataExplorer.FLUSH_IMMEDIATE));
        assertTrue(pd.contains(org.apache.nifi.processors.adx.IngestAzureDataExplorer.MAPPING_NAME));
        assertTrue(pd.contains(org.apache.nifi.processors.adx.IngestAzureDataExplorer.TABLE_NAME));
    }

    @Test
    void testMissingPropertyValuesOfAdxIngestProcessor() throws InitializationException {

        //missing property tableName
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.ADX_SERVICE,"adx-connection-service");

        testRunner.setValidateExpressionUsage(false);

        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.INGEST_URL,"http://ingest-sample.com/");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.APP_ID,"sample");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.APP_KEY,"sample");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.APP_TENANT,"sample");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.CLUSTER_URL, "http://sample.com/");

        testRunner.enableControllerService(kustoIngestService);
        testRunner.assertValid(kustoIngestService);
        AssertionFailedError assertionFailedError = assertThrows(AssertionFailedError.class,()->{
            testRunner.run(1);
        });
        assertTrue(assertionFailedError.getMessage().contains("Table Name"));
        assertTrue(assertionFailedError.getMessage().contains("invalid"));
    }

    @Test
    void testMissingPropertyValuesOfAdxConnectionService() throws InitializationException {

        //missing property tableName
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.MAPPING_NAME,MOCK_MAPPING_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.ADX_SERVICE,"adx-connection-service");

        testRunner.setValidateExpressionUsage(false);

        //missing ingest url required for connection service
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.APP_ID,"sample");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.APP_KEY,"sample");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.APP_TENANT,"sample");

        IllegalStateException illegalStateException =Assertions.assertThrows(IllegalStateException.class, () -> {
            testRunner.enableControllerService(kustoIngestService);
        });

        assertTrue(illegalStateException.getMessage().contains("Ingest URL"));
        assertTrue(illegalStateException.getMessage().contains("invalid"));

    }

    @Test
    void testIngestionFailure() throws InitializationException, IngestionClientException, IngestionServiceException {

        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.TABLE_NAME,MOCK_TABLE_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.MAPPING_NAME,MOCK_MAPPING_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.IngestAzureDataExplorer.ADX_SERVICE,"adx-connection-service");

        testRunner.setValidateExpressionUsage(false);

        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.INGEST_URL,"http://ingest-sample.com/");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.APP_ID,"sample");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.APP_KEY,"sample");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.APP_TENANT,"sample");
        testRunner.setProperty(kustoIngestService,StandardKustoIngestService.CLUSTER_URL, "http://sample.com/");

        testRunner.enableControllerService(kustoIngestService);
        testRunner.assertValid(kustoIngestService);

        KustoIngestService adxConnectionServiceFromProcessContext = (KustoIngestService) testRunner.getProcessContext().getControllerServiceLookup().getControllerService("adx-connection-service");
        assertNotNull(adxConnectionServiceFromProcessContext);

        InputStream stream = new ByteArrayInputStream("exampleString".getBytes(StandardCharsets.UTF_8));
        testRunner.enqueue(stream);
        try {
            testRunner.run(1);
        }catch (AssertionError e){
            Assertions.assertNotNull(e);
            Assertions.assertNotNull(e.getCause());
            Assertions.assertNotNull(e.getMessage());
            Assertions.assertTrue(e.getCause() instanceof  ProcessException);
        }
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.IngestAzureDataExplorer.RL_FAILED);


    }



}