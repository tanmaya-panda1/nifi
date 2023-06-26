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
package org.apache.nifi.adx.service;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.adx.AzureDataExplorerParameter;
import org.apache.nifi.adx.KustoIngestService;
import org.apache.nifi.adx.NiFiVersion;
import org.apache.nifi.adx.model.AzureDataExplorerConnectionParameters;
import org.apache.nifi.adx.model.KustoIngestionRequest;
import org.apache.nifi.adx.model.KustoIngestionResult;
import org.apache.nifi.adx.model.KustoQueryResponse;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Tags({"Azure", "ADX", "Kusto", "ingest", "azure"})
@CapabilityDescription("Sends batches of flowfile content or stream flowfile content to an Azure ADX cluster.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "AUTH_STRATEGY", description = "The strategy/method to authenticate against Azure Active Directory, either 'application' or 'managed_identity'."),
        @ReadsAttribute(attribute = "INGEST_URL", description = "Specifies the URL of ingestion endpoint of the Azure Data Explorer cluster."),
        @ReadsAttribute(attribute = "APP_ID", description = "Specifies Azure application id for accessing the ADX-Cluster."),
        @ReadsAttribute(attribute = "APP_KEY", description = "Specifies Azure application key for accessing the ADX-Cluster."),
        @ReadsAttribute(attribute = "APP_TENANT", description = "Azure application tenant for accessing the ADX-Cluster."),
        @ReadsAttribute(attribute = "CLUSTER_URL", description = "Endpoint of ADX cluster. This is required only when streaming data to ADX cluster is enabled."),
})
public class StandardKustoIngestService extends AbstractControllerService implements KustoIngestService {

    public static final PropertyDescriptor INGEST_URL = new PropertyDescriptor
            .Builder().name(AzureDataExplorerParameter.INGEST_URL.name())
            .displayName(AzureDataExplorerParameter.INGEST_URL.getDisplayName())
            .description(AzureDataExplorerParameter.INGEST_URL.getDescription())
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor KUSTO_AUTH_STRATEGY = new PropertyDescriptor
            .Builder().name(AzureDataExplorerParameter.AUTH_STRATEGY.name())
            .displayName(AzureDataExplorerParameter.AUTH_STRATEGY.getDisplayName())
            .description(AzureDataExplorerParameter.AUTH_STRATEGY.getDescription())
            .required(false)
            .defaultValue("application")
            .allowableValues("application", "managed_identity")
            .build();

    public static final PropertyDescriptor APP_ID = new PropertyDescriptor
            .Builder().name(AzureDataExplorerParameter.APP_ID.name())
            .displayName(AzureDataExplorerParameter.APP_ID.getDisplayName())
            .description(AzureDataExplorerParameter.APP_ID.getDescription())
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor APP_KEY = new PropertyDescriptor
            .Builder().name(AzureDataExplorerParameter.APP_KEY.name())
            .displayName(AzureDataExplorerParameter.APP_KEY.getDisplayName())
            .description(AzureDataExplorerParameter.APP_KEY.getDescription())
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor APP_TENANT = new PropertyDescriptor
            .Builder().name(AzureDataExplorerParameter.APP_TENANT.name())
            .displayName(AzureDataExplorerParameter.APP_TENANT.getDisplayName())
            .description(AzureDataExplorerParameter.APP_TENANT.getDescription())
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLUSTER_URL = new PropertyDescriptor
            .Builder().name(AzureDataExplorerParameter.CLUSTER_URL.name())
            .displayName(AzureDataExplorerParameter.CLUSTER_URL.getDisplayName())
            .description(AzureDataExplorerParameter.CLUSTER_URL.getDescription())
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(KUSTO_AUTH_STRATEGY, INGEST_URL, APP_ID, APP_KEY, APP_TENANT, CLUSTER_URL);

    private IngestClient ingestClient;

    private Client executionClient;

    private AzureDataExplorerConnectionParameters connectionParameters;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    public static final Pair<String, String> NIFI_SINK = Pair.of("processor", "nifi-sink");

    /**
     * @param context the configuration context
     * @throws InitializationException if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws ProcessException {

        getLogger().info("Starting Azure ADX Connection Service...");
        connectionParameters = new AzureDataExplorerConnectionParameters(context.getProperty(KUSTO_AUTH_STRATEGY).evaluateAttributeExpressions().getValue(),
                context.getProperty(APP_ID).evaluateAttributeExpressions().getValue(),
                context.getProperty(APP_KEY).evaluateAttributeExpressions().getValue(),
                context.getProperty(APP_TENANT).evaluateAttributeExpressions().getValue(),
                context.getProperty(CLUSTER_URL).evaluateAttributeExpressions().getValue(),
                context.getProperty(INGEST_URL).evaluateAttributeExpressions().getValue()
        );

        if (this.ingestClient == null) {
            this.ingestClient = createKustoIngestClient(this.connectionParameters.getKustoIngestionURL(), this.connectionParameters.getAppId(), this.connectionParameters.getAppKey(), this.connectionParameters.getAppTenant(), false, this.connectionParameters.getKustoEngineURL(), this.connectionParameters.getKustoAuthStrategy());
        }

        if(this.executionClient == null) {
            this.executionClient = createKustoExecutionClient(this.connectionParameters.getKustoEngineURL(), this.connectionParameters.getAppId(), this.connectionParameters.getAppKey(), this.connectionParameters.getAppTenant(), this.connectionParameters.getKustoAuthStrategy());
        }

    }

    @OnStopped
    public final void onStopped() {
        if (this.ingestClient != null) {
            try {
                this.ingestClient.close();
            } catch (IOException e) {
                getLogger().error("Closing Azure ADX Client failed with: " + e.getMessage(), e);
            } finally {
                this.ingestClient = null;
            }
        }
        this.executionClient = null;
    }


    protected IngestClient createKustoIngestClient(final String ingestUrl,
                                                final String appId,
                                                final String appKey,
                                                final String appTenant,
                                                final Boolean isStreamingEnabled,
                                                final String kustoEngineUrl,
                                                final String kustoAuthStrategy) {
        IngestClient kustoIngestClient;
        try {
            ConnectionStringBuilder ingestConnectionStringBuilder = createKustoEngineConnectionString(ingestUrl, appId, appKey, appTenant, kustoAuthStrategy);

            if (isStreamingEnabled) {
                ConnectionStringBuilder streamingConnectionStringBuilder = createKustoEngineConnectionString(kustoEngineUrl, appId, appKey, appTenant, kustoAuthStrategy);
                kustoIngestClient = IngestClientFactory.createManagedStreamingIngestClient(ingestConnectionStringBuilder, streamingConnectionStringBuilder);
            } else {
                kustoIngestClient = IngestClientFactory.createClient(ingestConnectionStringBuilder);
            }
        } catch (Exception e) {
            throw new ProcessException("Failed to initialize KustoIngestClient", e);
        }
        return kustoIngestClient;
    }

    public KustoIngestionResult ingestData(KustoIngestionRequest kustoIngestionRequest) {
        StreamSourceInfo info = new StreamSourceInfo(kustoIngestionRequest.getInputStream());
        if (kustoIngestionRequest.isStreamingEnabled()) {
            this.ingestClient = createKustoIngestClient(this.connectionParameters.getKustoIngestionURL(),
                    this.connectionParameters.getAppId(),
                    this.connectionParameters.getAppKey(),
                    this.connectionParameters.getAppTenant(),
                    true,
                    this.connectionParameters.getKustoEngineURL(),
                    this.connectionParameters.getKustoAuthStrategy());
        }

        //ingest data
        IngestionResult ingestionResult = null;
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        try {
            ingestionResult = ingestClient.ingestFromStream(info, kustoIngestionRequest.getIngestionProperties());
            List<IngestionStatus> statuses;
            CompletableFuture<List<IngestionStatus>> future = new CompletableFuture<>();
            IngestionResult finalIngestionResult = ingestionResult;
            Runnable task = () -> {
                try {
                    List<IngestionStatus> statuses1 = finalIngestionResult.getIngestionStatusCollection();
                    if (statuses1.get(0).status == OperationStatus.Succeeded
                            || statuses1.get(0).status == OperationStatus.Failed
                            || statuses1.get(0).status == OperationStatus.PartiallySucceeded) {
                        future.complete(statuses1);
                    }
                } catch (Exception e) {
                    future.completeExceptionally(new ProcessException("Error occurred while checking ingestion status", e));
                }
            };
            scheduler.scheduleWithFixedDelay(task, 1, 2, TimeUnit.SECONDS);
            statuses = future.get(1800, TimeUnit.SECONDS);
            return KustoIngestionResult.fromString(statuses.get(0).status.toString());
        } catch (IngestionClientException | IngestionServiceException e) {
            throw new ProcessException("Error occurred while ingesting data into ADX", e);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new ProcessException("Error occurred while checking ingestion status", e);
        } finally {
            shutDownScheduler(scheduler);
        }
    }

    public void shutDownScheduler(ScheduledExecutorService executorService) {
        executorService.shutdown();
        try {
            // Wait a while for existing tasks to terminate
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                // Cancel currently executing tasks forcefully
                executorService.shutdownNow();
                // Wait a while for tasks to respond to being cancelled
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS))
                    getLogger().error("Scheduler did not terminate");
            }
        } catch (InterruptedException ex) {
            // (Re-)Cancel if current thread also interrupted
            executorService.shutdownNow();
        }
    }

    private ConnectionStringBuilder createKustoEngineConnectionString(final String clusterUrl, final String appId, final String appKey, final String appTenant, final String kustoAuthStrategy) {
        final ConnectionStringBuilder kcsb;
        switch (kustoAuthStrategy) {
            case "application":
                if (StringUtils.isNotEmpty(appId) && StringUtils.isNotEmpty(appKey) && StringUtils.isNotEmpty(appTenant)) {
                    kcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                            clusterUrl,
                            appId,
                            appKey,
                            appTenant);
                } else {
                    throw new ProcessException("Kusto authentication missing App Credentials.");
                }
                break;

            case "managed_identity":
                kcsb = ConnectionStringBuilder.createWithAadManagedIdentity(
                        clusterUrl,
                        appId);
                break;

            default:
                throw new ProcessException("Failed to initialize KustoIngestClient, please " +
                        "provide valid credentials. Either Kusto managed identity or " +
                        "Kusto appId, appKey, and authority should be configured.");
        }
        kcsb.setConnectorDetails(NiFiVersion.CLIENT_NAME, NiFiVersion.getVersion(), null, null,
                false, null, NIFI_SINK);
        return kcsb;
    }

    protected Client createKustoExecutionClient(final String clusterUrl, final String appId, final String appKey, final String appTenant, final String kustoAuthStrategy) {
        getLogger().info("Creating KustoExecutionClient for clusterUrl: " + clusterUrl + " appId: " + appId + " appTenant: " + appTenant + " kustoAuthStrategy: " + kustoAuthStrategy);
        try {
            return ClientFactory.createClient(createKustoEngineConnectionString(clusterUrl, appId, appKey, appTenant, kustoAuthStrategy));
        } catch (Exception e) {
            throw new ProcessException("Failed to initialize KustoEngineClient", e);
        }
    }

    @Override
    public KustoQueryResponse executeQuery(String databaseName, String query) {
        KustoQueryResponse kustoQueryResponse;
        try {
            KustoResultSetTable kustoResultSetTable = this.executionClient.execute(databaseName, query).getPrimaryResults();
            kustoQueryResponse = new KustoQueryResponse(kustoResultSetTable);
        } catch (DataServiceException | DataClientException e) {
            getLogger().error("ADX Ingestion : Kusto Query execution failed", e);
            kustoQueryResponse = new KustoQueryResponse(true,e.getMessage());
        }
        return kustoQueryResponse;
    }

    public IngestClient getIngestClient() {
        return ingestClient;
    }

    public Client getExecutionClient() {
        return executionClient;
    }
}