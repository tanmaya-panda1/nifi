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
package org.apache.nifi.adx;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.KustoServiceQueryError;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import org.apache.nifi.adx.mock.MockStandardKustoIngestionService;
import org.apache.nifi.adx.model.KustoIngestionRequest;
import org.apache.nifi.adx.model.KustoIngestionResult;
import org.apache.nifi.adx.model.KustoQueryResponse;
import org.apache.nifi.adx.service.StandardKustoIngestService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardKustoIngestServiceTest {

    private TestRunner runner;

    private StandardKustoIngestService service;

    private static final String MOCK_URI = "https://mockURI:443/";
    private static final String MOCK_APP_ID = "mockAppId";

    private static final String MOCK_APP_KEY = "mockAppKey";

    private static final String MOCK_APP_TENANT = "mockAppTenant";

    private static final String MOCK_CLUSTER_URL = "https://mockClusterUrl.com/";

    @BeforeEach
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new MockStandardKustoIngestionService();
        runner.addControllerService("test-good", service);
    }

    @AfterEach
    public void after() {
        runner.clearProperties();
    }

    private void configureIngestURL() {
        runner.setProperty(service, StandardKustoIngestService.INGEST_URL, MOCK_URI);
    }

    private void configureAppId() {
        runner.setProperty(service, StandardKustoIngestService.APP_ID, MOCK_APP_ID);
    }

    private void configureAppKey() {
        runner.setProperty(service, StandardKustoIngestService.APP_KEY, MOCK_APP_KEY);
    }

    private void configureAppTenant() {
        runner.setProperty(service, StandardKustoIngestService.APP_TENANT, MOCK_APP_TENANT);
    }

    private void configureClusterURL() {
        runner.setProperty(service, StandardKustoIngestService.CLUSTER_URL, MOCK_CLUSTER_URL);
    }

    @Test
    void testAdxConnectionController() {
        configureIngestURL();
        configureAppId();
        configureAppKey();
        configureAppTenant();
        configureClusterURL();

        runner.assertValid(service);
    }

    @Test
    void testCreateIngestClient(){
        configureIngestURL();
        configureAppId();
        configureAppKey();
        configureAppTenant();
        configureClusterURL();

        runner.assertValid(service);
        runner.setValidateExpressionUsage(false);

        runner.enableControllerService(service);

        IngestClient ingestClient = service.getIngestClient();
        Assertions.assertNotNull(ingestClient);
    }

    @Test
    void testCreateExecutionClient(){
        configureIngestURL();
        configureAppId();
        configureAppKey();
        configureAppTenant();
        configureClusterURL();

        runner.assertValid(service);
        runner.setValidateExpressionUsage(false);

        runner.enableControllerService(service);

        Client executionClient = service.getExecutionClient();
        Assertions.assertNotNull(executionClient);
    }

    @Test
    void testPropertyDescriptor(){
        configureIngestURL();
        configureAppId();
        configureAppKey();
        configureAppTenant();

        List<PropertyDescriptor> pd = service.getSupportedPropertyDescriptors();
        assertTrue(pd.contains(StandardKustoIngestService.APP_ID));
        assertTrue(pd.contains(StandardKustoIngestService.APP_KEY));
        assertTrue(pd.contains(StandardKustoIngestService.INGEST_URL));
        assertTrue(pd.contains(StandardKustoIngestService.APP_TENANT));
        assertTrue(pd.contains(StandardKustoIngestService.CLUSTER_URL));
    }


    @Test
    void testIngestData() throws URISyntaxException, IngestionClientException, IngestionServiceException {
        configureIngestURL();
        configureAppId();
        configureAppKey();
        configureAppTenant();
        configureClusterURL();

        runner.assertValid(service);
        runner.setValidateExpressionUsage(false);

        runner.enableControllerService(service);

        IngestClient ingestClient = service.getIngestClient();
        Assertions.assertNotNull(ingestClient);

        InputStream is = Mockito.mock(InputStream.class);
        IngestionProperties ingestionProperties = Mockito.mock(IngestionProperties.class);
        IngestionResult ingestionResult = Mockito.mock(IngestionResult.class);
        IngestionStatus ingestionStatus = new IngestionStatus();
        ingestionStatus.setStatus(OperationStatus.Succeeded);
        Mockito.when(ingestionResult.getIngestionStatusCollection()).thenReturn(List.of(ingestionStatus));
        Mockito.when(ingestClient.ingestFromStream( Mockito.any(), Mockito.any())).thenReturn(ingestionResult);
        KustoIngestionResult kustoIngestionResult =service.ingestData(new KustoIngestionRequest(false, is ,ingestionProperties ));
        Assertions.assertNotNull(kustoIngestionResult);
        Assertions.assertEquals(OperationStatus.Succeeded.toString(), kustoIngestionResult.getStatus());
    }

    @Test
    void testIngestDataException() throws URISyntaxException, IngestionClientException, IngestionServiceException {
        configureIngestURL();
        configureAppId();
        configureAppKey();
        configureAppTenant();
        configureClusterURL();

        runner.assertValid(service);
        runner.setValidateExpressionUsage(false);

        runner.enableControllerService(service);

        IngestClient ingestClient = service.getIngestClient();
        Assertions.assertNotNull(ingestClient);

        InputStream is = Mockito.mock(InputStream.class);
        IngestionProperties ingestionProperties = Mockito.mock(IngestionProperties.class);
        IngestionResult ingestionResult = Mockito.mock(IngestionResult.class);
        IngestionStatus ingestionStatus = new IngestionStatus();
        ingestionStatus.setStatus(OperationStatus.Succeeded);
        Mockito.when(ingestionResult.getIngestionStatusCollection()).thenReturn(List.of(ingestionStatus));
        Mockito.when(ingestClient.ingestFromStream( Mockito.any(), Mockito.any())).thenThrow(new IngestionServiceException("test"));
        Assertions.assertThrows(ProcessException.class, () -> service.ingestData(new KustoIngestionRequest(false, is ,ingestionProperties )));
    }

    @Test
    void testExecuteQuery() throws DataServiceException, DataClientException, KustoServiceQueryError {

        configureIngestURL();
        configureAppId();
        configureAppKey();
        configureAppTenant();
        configureClusterURL();

        runner.assertValid(service);
        runner.setValidateExpressionUsage(false);

        runner.enableControllerService(service);

        Client executionClient = service.getExecutionClient();
        Assertions.assertNotNull(executionClient);

        KustoResultSetTable kustoResultSetTable = Mockito.mock(KustoResultSetTable.class);
        KustoOperationResult kustoOperationResult = Mockito.mock(KustoOperationResult.class);
        Mockito.when(executionClient.execute( Mockito.any(), Mockito.any())).thenReturn(kustoOperationResult);
        Mockito.when(kustoOperationResult.getPrimaryResults()).thenReturn(kustoResultSetTable);

        KustoQueryResponse kustoQueryResponse = service.executeQuery("test", "test");
        Assertions.assertNotNull(kustoQueryResponse);
    }

    @Test
    void testExecuteQueryException() throws DataServiceException, DataClientException {

        configureIngestURL();
        configureAppId();
        configureAppKey();
        configureAppTenant();
        configureClusterURL();

        runner.assertValid(service);
        runner.setValidateExpressionUsage(false);

        runner.enableControllerService(service);

        Client executionClient = service.getExecutionClient();
        Assertions.assertNotNull(executionClient);

        KustoResultSetTable kustoResultSetTable = Mockito.mock(KustoResultSetTable.class);
        KustoOperationResult kustoOperationResult = Mockito.mock(KustoOperationResult.class);
        Mockito.when(executionClient.execute( Mockito.any(), Mockito.any())).thenThrow(new DataClientException("test","test"));

        KustoQueryResponse kustoQueryResponse = service.executeQuery("test", "test");
        Assertions.assertTrue(kustoQueryResponse.isError());
    }


}