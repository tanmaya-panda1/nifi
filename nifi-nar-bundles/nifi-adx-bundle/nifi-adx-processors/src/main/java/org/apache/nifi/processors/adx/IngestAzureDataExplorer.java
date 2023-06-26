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

import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.adx.KustoIngestService;
import org.apache.nifi.adx.model.KustoIngestionRequest;
import org.apache.nifi.adx.model.KustoIngestionResult;
import org.apache.nifi.adx.model.KustoQueryResponse;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.adx.enums.AzureAdxSinkProcessorParameter;
import org.apache.nifi.processors.adx.enums.IngestionDataFormat;
import org.apache.nifi.processors.adx.enums.RelationshipStatus;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Tags({"azure", "adx", "microsoft", "data", "explorer"})
@CapabilityDescription("The Azure ADX Ingest Processor acts as a ADX sink connector which sends flowFiles using the ADX-Service to the provided Azure Data" +
        "Explorer Ingest Endpoint. The data can be sent through queued ingestion or streaming ingestion to the Azure Data Explorer cluster." +
        "The data ingested to ADX can be in non-transactional mode or transactional mode. " +
        "This processor supports transactionality of the ingested data ie. it ensures no duplicates are inserted while retries during ingestion failures. " +
        "But a word of caution while selecting transactional mode is, it significantly reduces the ingestion time " +
        "since the processor first tries to ingest the data into temporary table before ingesting to the main table. ")
@ReadsAttributes({
        @ReadsAttribute(attribute = "DB_NAME", description = "Specifies the name of the ADX database where the data needs to be stored."),
        @ReadsAttribute(attribute = "TABLE_NAME", description = "Specifies the name of the ADX table where the data needs to be stored."),
        @ReadsAttribute(attribute = "MAPPING_NAME", description = "Specifies the name of the mapping responsible for storing the data in appropriate columns."),
        @ReadsAttribute(attribute = "FLUSH_IMMEDIATE", description = "In case of queued ingestion, this property determines whether the data should be flushed immediately to the ingest endpoint."),
        @ReadsAttribute(attribute = "DATA_FORMAT", description = "Specifies the format of data that is send to Azure Data Explorer."),
        @ReadsAttribute(attribute = "IR_LEVEL", description = "ADX can report events on several levels. Ex- None, Failure and Failure & Success."),
        @ReadsAttribute(attribute = "IS_TRANSACTIONAL", description = "Default : No ,Incase of any failure, whether we want all our data ingested or none. " +
                "If set to Yes, it increases the data ingestion time significantly because inorder to maintain transactional behaviour, " +
                "the processor first tries to ingest into temporary tables before ingesting into actual table."),
        @ReadsAttribute(attribute = "IGNORE_FIRST_RECORD", description = "Specifies whether we want to ignore ingestion of first record. " +
                "This is primarily applicable for csv files. Default is set to NO"),
})
@Stateful(scopes = Scope.CLUSTER, description = "In case the user wants transactional property during data ingestion, " +
        "AzureIngestProcessor uses temporary tables to attempt ingestion initially and to store the ingestion status into temp tables of various nodes, it uses nifi statemanager")
public class IngestAzureDataExplorer extends AbstractProcessor {

    public static final String FETCH_TABLE_COMMAND = "%s | count";
    public static final String STREAMING_POLICY_SHOW_COMMAND = ".show %s %s policy streamingingestion";
    public static final String DATABASE = "database";

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private KustoIngestService service;
    private boolean isStreamingEnabled;

    public static final AllowableValue AVRO = new AllowableValue(
            IngestionDataFormat.AVRO.name(), IngestionDataFormat.AVRO.getExtension(),
            IngestionDataFormat.AVRO.getDescription());

    public static final AllowableValue APACHEAVRO = new AllowableValue(
            IngestionDataFormat.APACHEAVRO.name(), IngestionDataFormat.APACHEAVRO.getExtension(),
            IngestionDataFormat.APACHEAVRO.getDescription());

    public static final AllowableValue CSV = new AllowableValue(
            IngestionDataFormat.CSV.name(), IngestionDataFormat.CSV.getExtension(),
            IngestionDataFormat.CSV.getDescription());

    public static final AllowableValue JSON = new AllowableValue(
            IngestionDataFormat.JSON.name(), IngestionDataFormat.JSON.getExtension(),
            IngestionDataFormat.JSON.getDescription());

    public static final AllowableValue MULTIJSON = new AllowableValue(
            IngestionDataFormat.MULTIJSON.name(), IngestionDataFormat.MULTIJSON.getExtension(),
            IngestionDataFormat.MULTIJSON.getDescription());

    public static final AllowableValue ORC = new AllowableValue(
            IngestionDataFormat.ORC.name(), IngestionDataFormat.ORC.getExtension(), IngestionDataFormat.ORC.getDescription());

    public static final AllowableValue PARQUET = new AllowableValue(
            IngestionDataFormat.PARQUET.name(), IngestionDataFormat.PARQUET.getExtension(), IngestionDataFormat.PARQUET.getDescription());

    public static final AllowableValue PSV = new AllowableValue(
            IngestionDataFormat.PSV.name(), IngestionDataFormat.PSV.getExtension(), IngestionDataFormat.PSV.getDescription());

    public static final AllowableValue SCSV = new AllowableValue(
            IngestionDataFormat.SCSV.name(), IngestionDataFormat.SCSV.getExtension(), IngestionDataFormat.SCSV.getDescription());

    public static final AllowableValue SOHSV = new AllowableValue(
            IngestionDataFormat.SOHSV.name(), IngestionDataFormat.SOHSV.getExtension(),
            IngestionDataFormat.SOHSV.getDescription());

    public static final AllowableValue TSV = new AllowableValue(
            IngestionDataFormat.TSV.name(), IngestionDataFormat.TSV.getExtension(), IngestionDataFormat.TSV.getDescription());

    public static final AllowableValue TSVE = new AllowableValue(
            IngestionDataFormat.TSVE.name(), IngestionDataFormat.TSVE.getExtension(),
            IngestionDataFormat.TSVE.getDescription());

    public static final AllowableValue TXT = new AllowableValue(
            IngestionDataFormat.TXT.name(), IngestionDataFormat.TXT.getExtension(),
            IngestionDataFormat.TXT.getDescription());

    public static final AllowableValue TRANSACTIONAL_YES = new AllowableValue(
            "YES", "YES",
            "Transactionality required for ingestion");

    public static final AllowableValue TRANSACTIONAL_NO = new AllowableValue(
            "NO", "NO",
            "Transactionality not required for ingestion");

    public static final AllowableValue IGNORE_FIRST_RECORD_YES = new AllowableValue(
            "YES", "YES",
            "Ignore first record during ingestion");

    public static final AllowableValue IGNORE_FIRST_RECORD_NO = new AllowableValue(
            "NO", "NO",
            "Do not ignore first record during ingestion");

    public static final PropertyDescriptor DB_NAME = new PropertyDescriptor
            .Builder().name(AzureAdxSinkProcessorParameter.DB_NAME.name())
            .displayName(AzureAdxSinkProcessorParameter.DB_NAME.getDisplayName())
            .description(AzureAdxSinkProcessorParameter.DB_NAME.getParamDescription())
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor
            .Builder().name(AzureAdxSinkProcessorParameter.TABLE_NAME.name())
            .displayName(AzureAdxSinkProcessorParameter.TABLE_NAME.getDisplayName())
            .description(AzureAdxSinkProcessorParameter.TABLE_NAME.getParamDescription())
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAPPING_NAME = new PropertyDescriptor
            .Builder().name(AzureAdxSinkProcessorParameter.MAPPING_NAME.name())
            .displayName(AzureAdxSinkProcessorParameter.MAPPING_NAME.getDisplayName())
            .description(AzureAdxSinkProcessorParameter.MAPPING_NAME.getParamDescription())
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor IS_STREAMING_ENABLED = new PropertyDescriptor
            .Builder().name(AzureAdxSinkProcessorParameter.IS_STREAMING_ENABLED.name())
            .displayName(AzureAdxSinkProcessorParameter.IS_STREAMING_ENABLED.getDisplayName())
            .description(AzureAdxSinkProcessorParameter.IS_STREAMING_ENABLED.getParamDescription())
            .required(false)
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor ADX_SERVICE = new PropertyDescriptor
            .Builder().name(AzureAdxSinkProcessorParameter.ADX_SERVICE.name())
            .displayName(AzureAdxSinkProcessorParameter.ADX_SERVICE.getDisplayName())
            .description(AzureAdxSinkProcessorParameter.ADX_SERVICE.getParamDescription())
            .required(true)
            .identifiesControllerService(KustoIngestService.class)
            .build();
    public static final PropertyDescriptor SHOW_ADVANCED_OPTIONS = new PropertyDescriptor
            .Builder().name(AzureAdxSinkProcessorParameter.SHOW_ADVANCED_OPTIONS.name())
            .displayName(AzureAdxSinkProcessorParameter.SHOW_ADVANCED_OPTIONS.getDisplayName())
            .description(AzureAdxSinkProcessorParameter.SHOW_ADVANCED_OPTIONS.getParamDescription())
            .required(false)
            .allowableValues("YES", "NO")
            .defaultValue("NO")
            .build();
    public static final PropertyDescriptor IS_TRANSACTIONAL = new PropertyDescriptor
            .Builder().name(AzureAdxSinkProcessorParameter.IS_TRANSACTIONAL.name())
            .displayName(AzureAdxSinkProcessorParameter.IS_TRANSACTIONAL.getDisplayName())
            .description(AzureAdxSinkProcessorParameter.IS_TRANSACTIONAL.getParamDescription())
            .required(false)
            .allowableValues(TRANSACTIONAL_YES, TRANSACTIONAL_NO)
            .defaultValue(TRANSACTIONAL_NO.getDisplayName())
            .dependsOn(SHOW_ADVANCED_OPTIONS, "YES")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TEMP_TABLE_SOFT_DELETE_RETENTION = new PropertyDescriptor
            .Builder().name("TEMP_TABLE_SOFT_DELETE_RETENTION")
            .displayName("Temporary table soft delete retention period")
            .description("This property specifies the soft delete retention period of temporary table when data ingestion is selected in transactional mode")
            .dependsOn(IS_TRANSACTIONAL, TRANSACTIONAL_YES)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(SHOW_ADVANCED_OPTIONS, "YES")
            .defaultValue("1d")
            .build();
    public static final Relationship RL_SUCCEEDED = new Relationship.Builder()
            .name(RelationshipStatus.RL_SUCCEEDED.name())
            .description(RelationshipStatus.RL_SUCCEEDED.getDescription())
            .build();
    public static final Relationship RL_FAILED = new Relationship.Builder()
            .name(RelationshipStatus.RL_FAILED.name())
            .description(RelationshipStatus.RL_FAILED.getDescription())
            .build();
    static final PropertyDescriptor FLUSH_IMMEDIATE = new PropertyDescriptor.Builder()
            .name(AzureAdxSinkProcessorParameter.FLUSH_IMMEDIATE.name())
            .displayName(AzureAdxSinkProcessorParameter.FLUSH_IMMEDIATE.getDisplayName())
            .description(AzureAdxSinkProcessorParameter.FLUSH_IMMEDIATE.getParamDescription())
            .required(true)
            .defaultValue("false")
            .dependsOn(SHOW_ADVANCED_OPTIONS, "YES")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor DATA_FORMAT = new PropertyDescriptor.Builder()
            .name(AzureAdxSinkProcessorParameter.DATA_FORMAT.name())
            .displayName(AzureAdxSinkProcessorParameter.DATA_FORMAT.getDisplayName())
            .description(AzureAdxSinkProcessorParameter.DATA_FORMAT.getParamDescription())
            .required(true)
            .allowableValues(AVRO, APACHEAVRO, CSV, JSON, MULTIJSON, ORC, PARQUET, PSV, SCSV, SOHSV, TSV, TSVE, TXT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor IGNORE_FIRST_RECORD = new PropertyDescriptor.Builder()
            .name(AzureAdxSinkProcessorParameter.IS_IGNORE_FIRST_RECORD.name())
            .displayName(AzureAdxSinkProcessorParameter.IS_IGNORE_FIRST_RECORD.getDisplayName())
            .description(AzureAdxSinkProcessorParameter.IS_IGNORE_FIRST_RECORD.getParamDescription())
            .required(false)
            .allowableValues(IGNORE_FIRST_RECORD_YES, IGNORE_FIRST_RECORD_NO)
            .dependsOn(SHOW_ADVANCED_OPTIONS, "YES")
            .defaultValue(IGNORE_FIRST_RECORD_NO.getValue())
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptorList = new ArrayList<>();
        descriptorList.add(ADX_SERVICE);
        descriptorList.add(DB_NAME);
        descriptorList.add(TABLE_NAME);
        descriptorList.add(MAPPING_NAME);
        descriptorList.add(FLUSH_IMMEDIATE);
        descriptorList.add(DATA_FORMAT);
        descriptorList.add(IS_TRANSACTIONAL);
        descriptorList.add(IGNORE_FIRST_RECORD);
        descriptorList.add(TEMP_TABLE_SOFT_DELETE_RETENTION);
        descriptorList.add(IS_STREAMING_ENABLED);
        descriptorList.add(SHOW_ADVANCED_OPTIONS);
        this.descriptors = Collections.unmodifiableList(descriptorList);

        final Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(RL_SUCCEEDED);
        relationshipSet.add(RL_FAILED);
        this.relationships = Collections.unmodifiableSet(relationshipSet);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        service = context.getProperty(ADX_SERVICE).asControllerService(KustoIngestService.class);
        if (checkIfIngestorRoleDoesntExist(context.getProperty(DB_NAME).getValue(), context.getProperty(TABLE_NAME).getValue())) {
            getLogger().error("User might not have ingestor privileges, table validation will be skipped for all table mappings.");
            throw new ProcessException("User might not have ingestor privileges, table validation will be skipped for all table mappings. ");
        }
        isStreamingEnabled = context.getProperty(IS_STREAMING_ENABLED).evaluateAttributeExpressions().asBoolean();
        if (isStreamingEnabled) {
            checkIfStreamingPolicyIsEnabledInADX(context.getProperty(DB_NAME).getValue(), context.getProperty(DB_NAME).getValue());
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            context.yield();
            return;
        }

        IngestionProperties ingestionProperties = new IngestionProperties(context.getProperty(DB_NAME).getValue(),
                context.getProperty(TABLE_NAME).getValue());

        IngestionMapping.IngestionMappingKind ingestionMappingKind = null;

        isStreamingEnabled = context.getProperty(IS_STREAMING_ENABLED).evaluateAttributeExpressions().asBoolean();

        switch (IngestionDataFormat.valueOf(context.getProperty(DATA_FORMAT).getValue())) {
            case AVRO:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.AVRO);
                ingestionMappingKind = IngestionProperties.DataFormat.AVRO.getIngestionMappingKind();
                break;
            case APACHEAVRO:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.APACHEAVRO);
                ingestionMappingKind = IngestionProperties.DataFormat.APACHEAVRO.getIngestionMappingKind();
                break;
            case CSV:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.CSV);
                ingestionMappingKind = IngestionProperties.DataFormat.CSV.getIngestionMappingKind();
                break;
            case JSON:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
                ingestionMappingKind = IngestionProperties.DataFormat.JSON.getIngestionMappingKind();
                break;
            case MULTIJSON:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.MULTIJSON);
                ingestionMappingKind = IngestionProperties.DataFormat.MULTIJSON.getIngestionMappingKind();
                break;
            case ORC:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.ORC);
                ingestionMappingKind = IngestionProperties.DataFormat.ORC.getIngestionMappingKind();
                break;
            case PARQUET:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.PARQUET);
                ingestionMappingKind = IngestionProperties.DataFormat.PARQUET.getIngestionMappingKind();
                break;
            case PSV:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.PSV);
                ingestionMappingKind = IngestionProperties.DataFormat.PSV.getIngestionMappingKind();
                break;
            case SCSV:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.SCSV);
                ingestionMappingKind = IngestionProperties.DataFormat.SCSV.getIngestionMappingKind();
                break;
            case SOHSV:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.SOHSV);
                ingestionMappingKind = IngestionProperties.DataFormat.SOHSV.getIngestionMappingKind();
                break;
            case TSV:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.TSV);
                ingestionMappingKind = IngestionProperties.DataFormat.TSV.getIngestionMappingKind();
                break;
            case TSVE:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.TSVE);
                ingestionMappingKind = IngestionProperties.DataFormat.TSVE.getIngestionMappingKind();
                break;
            case TXT:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.TXT);
                ingestionMappingKind = IngestionProperties.DataFormat.TXT.getIngestionMappingKind();
                break;
        }

        if (StringUtils.isNotEmpty(context.getProperty(MAPPING_NAME).getValue()) && ingestionMappingKind != null) {
            ingestionProperties.setIngestionMapping(context.getProperty(MAPPING_NAME).getValue(), ingestionMappingKind);
        }

        ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES);

        ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE);

        ingestionProperties.setFlushImmediately(StringUtils.equalsIgnoreCase(context.getProperty(FLUSH_IMMEDIATE).getValue(), "true"));

        ingestionProperties.setIgnoreFirstRecord(StringUtils.equalsIgnoreCase(context.getProperty(IGNORE_FIRST_RECORD).getValue(), IGNORE_FIRST_RECORD_YES.getValue()));

        boolean isSingleNodeTempTableIngestionSucceeded = false;
        boolean isClusteredTempTableIngestionSucceeded = false;
        boolean isError = false;

        IngestionProperties ingestionPropertiesCreateTempTable;

        if (StringUtils.equalsIgnoreCase(context.getProperty(IS_TRANSACTIONAL).getValue(), TRANSACTIONAL_YES.getValue()) && !isStreamingEnabled) {
            String tempTableName = context.getProperty(TABLE_NAME).getValue() + "_tmp" + new Timestamp(System.currentTimeMillis()).getTime();

            ingestionPropertiesCreateTempTable = new IngestionProperties(context.getProperty(DB_NAME).getValue(), tempTableName);
            ingestionPropertiesCreateTempTable.setDataFormat(ingestionProperties.getDataFormat());
            ingestionPropertiesCreateTempTable.setIngestionMapping(ingestionProperties.getIngestionMapping());
            ingestionPropertiesCreateTempTable.setReportLevel(ingestionProperties.getReportLevel());
            ingestionPropertiesCreateTempTable.setReportMethod(ingestionProperties.getReportMethod());
            ingestionPropertiesCreateTempTable.setIgnoreFirstRecord(ingestionProperties.isIgnoreFirstRecord());
            ingestionPropertiesCreateTempTable.setFlushImmediately(ingestionProperties.getFlushImmediately());

            try (final InputStream in = session.read(flowFile)) {
                //check if it is transactional
                Map<String, String> stateMap = null;
                //if clustered - update in statemap status as inprogress for that nodeId
                if (isNifiClusteredSetup(getNodeTypeProvider())) {
                    StateManager stateManager = context.getStateManager();
                    if (stateManager.getState(Scope.CLUSTER).toMap().isEmpty()) {
                        stateMap = new ConcurrentHashMap<>();
                        stateMap.put(getNodeTypeProvider().getCurrentNode().toString(), "IN_PROGRESS");
                        stateManager.setState(stateMap, Scope.CLUSTER);
                    } else {
                        Map<String, String> existingMap = stateManager.getState(Scope.CLUSTER).toMap();
                        Map<String, String> updatedMap = new ConcurrentHashMap<>(existingMap);
                        updatedMap.put(getNodeTypeProvider().getCurrentNode().toString(), "IN_PROGRESS");
                        stateManager.setState(updatedMap, Scope.CLUSTER);
                    }
                    getLogger().info("StateMap  - {}", stateManager.getState(Scope.CLUSTER).toMap());
                }

                Calendar calendar = Calendar.getInstance();
                calendar.add(Calendar.DATE, 1);
                String expiryDate = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime());

                //create temp table
                createTempTable(ingestionPropertiesCreateTempTable, ingestionProperties);

                //alter retention policy of temp table
                alterTempTableRetentionPolicy(ingestionPropertiesCreateTempTable, context);

                //alter auto delete policy of temp table
                alterTempTableAutoDeletePolicy(ingestionPropertiesCreateTempTable, expiryDate);

                //ingest data
                KustoIngestionResult tempTableIngestionResult = service.ingestData(new KustoIngestionRequest(isStreamingEnabled, in, ingestionPropertiesCreateTempTable));

                if (tempTableIngestionResult == KustoIngestionResult.SUCCEEDED) {
                    getLogger().info("Operation status Succeeded in temp table - {}", tempTableIngestionResult);
                    //if clustered and if ingestion succeeded then update status to success and if non clustered set flag to tempTableIngestion succeeded
                    if (isNifiClusteredSetup(getNodeTypeProvider())) {
                        Map<String, String> existingMap = context.getStateManager().getState(Scope.CLUSTER).toMap();
                        Map<String, String> updatedMap = new ConcurrentHashMap<>(existingMap);
                        updatedMap.put(getNodeTypeProvider().getCurrentNode().toString(), "SUCCEEDED");
                        context.getStateManager().setState(updatedMap, Scope.CLUSTER);
                    } else {
                        isSingleNodeTempTableIngestionSucceeded = true;
                    }
                }

                if (tempTableIngestionResult == KustoIngestionResult.FAILED || tempTableIngestionResult == KustoIngestionResult.PARTIALLY_SUCCEEDED) {
                    getLogger().error("Operation status Error - {}", tempTableIngestionResult);
                    if (isNifiClusteredSetup(getNodeTypeProvider())) {
                        Map<String, String> existingMap = context.getStateManager().getState(Scope.CLUSTER).toMap();
                        Map<String, String> updatedMap = new ConcurrentHashMap<>(existingMap);
                        updatedMap.put(getNodeTypeProvider().getCurrentNode().toString(), "FAILED");
                        context.getStateManager().setState(updatedMap, Scope.CLUSTER);
                    }
                }

                //if clustered check if the all the nodes ingestion status succeeded
                //no of nodes in the cluster and success should be same
                //if yes proceed for ingestion to actual table
                //if pending, wait for sometime, with configurable timeout
                //if all failed/partially succeeded then rel-failure

                if (isNifiClusteredSetup(getNodeTypeProvider())) {
                    CompletableFuture<Integer> countFuture = new CompletableFuture<>();
                    ScheduledExecutorService countScheduler = Executors.newScheduledThreadPool(1);

                    Runnable countTask = () -> {
                        try {
                            Map<String, String> nodeMap = context.getStateManager().getState(Scope.CLUSTER).toMap();
                            int pendingCount = nodeMap.size();
                            int succeededCount = 0;
                            for (Map.Entry<String, String> entry : nodeMap.entrySet()) {
                                if (entry.getValue().equals("SUCCEEDED")) {
                                    succeededCount++;
                                    pendingCount--;
                                    getLogger().error("Statemap inside loop values succeeded - {} and {}", succeededCount, pendingCount);
                                } else if (entry.getValue().equals("FAILED")) {
                                    pendingCount--;
                                    getLogger().error("Statemap inside loop values failed - {}", pendingCount);
                                }
                            }
                            if (pendingCount == 0) {
                                getLogger().error("Statemap inside completed task execution - {} and {}", succeededCount, pendingCount);
                                countFuture.complete(succeededCount);
                            }
                        } catch (Exception e) {
                            countFuture.completeExceptionally(new ProcessException("Error occurred while checking ingestion status", e));
                        }
                    };

                    countScheduler.scheduleWithFixedDelay(countTask, 1L, 2L, TimeUnit.SECONDS);
                    int succeededCount = countFuture.get(1800, TimeUnit.SECONDS);

                    shutDownScheduler(countScheduler);

                    if (succeededCount == context.getStateManager().getState(Scope.CLUSTER).toMap().size()) {
                        //clustered temp table ingestion succeeds
                        getLogger().error("Clustered Ingestion : succededCount same as state size " + succeededCount);
                        isClusteredTempTableIngestionSucceeded = true;
                    } else {
                        //clustered temp table ingestion fails
                        getLogger().error("Clustered Ingestion : Exception occurred while ingesting data into the ADX temp tables, hence aborting ingestion to main table.");
                        isError = true;
                    }
                }

                if (isClusteredTempTableIngestionSucceeded || isSingleNodeTempTableIngestionSucceeded) {
                    //move extents from temp table to actual table
                    StringBuilder moveExtentsQuery = new StringBuilder().append(".move async extents all from table ").append(ingestionPropertiesCreateTempTable.getTableName()).append(" to table ").append(ingestionProperties.getTableName());
                    if (shouldUseMaterializedViewFlag(ingestionPropertiesCreateTempTable.getDatabaseName(), ingestionPropertiesCreateTempTable.getTableName())) {
                        moveExtentsQuery.append(" with(SetNewIngestionTime=true)");
                    }
                    String operationId = executeMoveExtentsAsyncOperation(ingestionPropertiesCreateTempTable.getDatabaseName(), moveExtentsQuery.toString());
                    String showOperationsQuery = ".show operations " + operationId;
                    String stateCol = "State";
                    String completionStatus = pollAndFindExtentMergeAsyncOperation(ingestionPropertiesCreateTempTable.getDatabaseName(), showOperationsQuery, stateCol);
                    if (completionStatus.equalsIgnoreCase("Failed")) {
                        getLogger().error("Error occurred while moving extents from temp tables to actual table");
                        isError = true;
                    }
                } else {
                    isError = true;
                }

            } catch (ExecutionException | TimeoutException | IOException | InterruptedException e) {
                throw new ProcessException("Transactional Mode : Exception occurred while ingesting data into ADX with exception {}",e);
            } finally {
                dropTempTableIfExists(ingestionPropertiesCreateTempTable);
                if (isNifiClusteredSetup(getNodeTypeProvider())) {
                    try {
                        context.getStateManager().clear(Scope.CLUSTER);
                    } catch (IOException e) {
                        getLogger().error("Exception occurred while clearing the cluster state {} ", e);
                        isError = true;
                    }
                }
            }
        } else {
            // when the transactional flag is false or streaming ingestion
            try (final InputStream inputStream = session.read(flowFile)) {
                StringBuilder ingestLogString = new StringBuilder().append("Ingesting with: ")
                        .append("dataFormat - ").append(ingestionProperties.getDataFormat()).append("|")
                        .append("ingestionMapping - ").append(ingestionProperties.getIngestionMapping().getIngestionMappingReference()).append("|")
                        .append("reportLevel - ").append(ingestionProperties.getReportLevel()).append("|")
                        .append("reportMethod - ").append(ingestionProperties.getReportMethod()).append("|")
                        .append("databaseName - ").append(ingestionProperties.getDatabaseName()).append("|")
                        .append("tableName - ").append(ingestionProperties.getTableName()).append("|")
                        .append("flushImmediately - ").append(ingestionProperties.getFlushImmediately());
                getLogger().info(ingestLogString.toString());

                KustoIngestionResult result = service.ingestData(new KustoIngestionRequest(isStreamingEnabled, inputStream, ingestionProperties));

                getLogger().info("Operation status: {} ", result.toString());
                if (result == KustoIngestionResult.SUCCEEDED) {
                    getLogger().info("Operation status Succeeded - {}", result.toString());
                }

                if (result == KustoIngestionResult.FAILED) {
                    getLogger().error("Operation status Error - {}", result.toString());
                    isError = true;
                }

                if (result == KustoIngestionResult.PARTIALLY_SUCCEEDED) {
                    getLogger().error("Operation status Partially succeeded - {}", result.toString());
                    isError = true;
                }
            } catch (IOException e) {
                getLogger().error("Non Transactional/Streaming Ingestion mode : Exception occurred while ingesting data into ADX with exception {} ", e);
                isError = true;
            }
        }

        if (isError) {
            getLogger().error("Process failed - {}");
            session.transfer(flowFile, RL_FAILED);
        } else {
            getLogger().info("Process succeeded - {}");
            session.transfer(flowFile, RL_SUCCEEDED);
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

    protected void checkIfStreamingPolicyIsEnabledInADX(String entityName, String database) {
        KustoQueryResponse kustoQueryResponse = service.executeQuery(database, String.format(STREAMING_POLICY_SHOW_COMMAND, IngestAzureDataExplorer.DATABASE, entityName));
        if (kustoQueryResponse.isError()) {
            throw new ProcessException("Error occurred while checking if streaming policy is enabled for the table");
        }
        KustoResultSetTable ingestionResultSet = kustoQueryResponse.getIngestionResultSet();
        ingestionResultSet.next();
        ingestionResultSet.getString("Policy");
    }

    protected boolean checkIfIngestorRoleDoesntExist(String databaseName, String tableName) {
        KustoQueryResponse kustoQueryResponse = service.executeQuery(databaseName, String.format(FETCH_TABLE_COMMAND, tableName));
        return kustoQueryResponse.isError();
    }

    protected String showOriginalTableRetentionPolicy(IngestionProperties ingestionProperties) {
        String showTableSchema = ".show table " + ingestionProperties.getTableName() + " cslschema";
        KustoQueryResponse kustoQueryResponse = service.executeQuery(ingestionProperties.getDatabaseName(), showTableSchema);
        if (kustoQueryResponse.isError()) {
            throw new ProcessException("Error occurred while showing original table retention policy");
        }
        KustoResultSetTable kustoOperationResult = kustoQueryResponse.getIngestionResultSet();
        String columnsAsSchema = null;
        if (kustoOperationResult.first()) {
            int columnIndex = kustoOperationResult.findColumn("Schema");
            columnsAsSchema = kustoOperationResult.getString(columnIndex);
        }
        return columnsAsSchema;
    }

    protected void dropTempTableIfExists(IngestionProperties ingestionPropertiesCreateTempTable) throws ProcessException {
        String dropTempTableIfExistsQuery = ".drop table " + ingestionPropertiesCreateTempTable.getTableName() + " ifexists";
        KustoQueryResponse kustoQueryResponse = service.executeQuery(ingestionPropertiesCreateTempTable.getDatabaseName(), dropTempTableIfExistsQuery);
        if (kustoQueryResponse.isError()) {
            throw new ProcessException("Error occurred while dropping the temp table");
        }
    }

    protected void createTempTable(IngestionProperties ingestionPropertiesCreateTempTable, IngestionProperties ingestionProperties) throws ProcessException {
        String createTempTableQuery = ".create table " + ingestionPropertiesCreateTempTable.getTableName()
                + " based-on " + ingestionProperties.getTableName() + " with (docstring='sample-table', folder='TempTables', hidden=true) ";
        KustoQueryResponse kustoQueryResponse = service.executeQuery(ingestionPropertiesCreateTempTable.getDatabaseName(), createTempTableQuery);
        if (kustoQueryResponse.isError()) {
            throw new ProcessException("Error occurred while creating the temp table");
        }
    }

    protected void alterTempTableRetentionPolicy(IngestionProperties ingestionPropertiesCreateTempTable, ProcessContext context) throws ProcessException {
        String alterRetentionPolicyTempTableQuery =
                ".alter-merge table " + ingestionPropertiesCreateTempTable.getTableName() + " policy retention softdelete ="
                        + context.getProperty(TEMP_TABLE_SOFT_DELETE_RETENTION).getValue() + " recoverability = disabled";
        KustoQueryResponse kustoQueryResponse = service.executeQuery(ingestionPropertiesCreateTempTable.getDatabaseName(), alterRetentionPolicyTempTableQuery);
        if (kustoQueryResponse.isError()) {
            throw new ProcessException("Error occurred while setting retention policy for the temp table");
        }
    }

    protected void alterTempTableAutoDeletePolicy(IngestionProperties ingestionPropertiesCreateTempTable, String expiryDate) throws ProcessException {
        String setAutoDeleteForTempTableQuery =
                ".alter table " + ingestionPropertiesCreateTempTable.getTableName() + " policy auto_delete @'{ \"ExpiryDate\" : \"" + expiryDate + "\", \"DeleteIfNotEmpty\": true }'";
        KustoQueryResponse kustoQueryResponse = service.executeQuery(ingestionPropertiesCreateTempTable.getDatabaseName(), setAutoDeleteForTempTableQuery);
        if (kustoQueryResponse.isError()) {
            throw new ProcessException("Error occurred while setting auto delete policy for the temp table");
        }
    }

    protected boolean isNifiClusteredSetup(NodeTypeProvider nodeTypeProvider) {
        return nodeTypeProvider.isClustered() && nodeTypeProvider.isConnected();
    }

    protected boolean shouldUseMaterializedViewFlag(String databaseName, String tableName) throws ProcessException {
        String materializedViewQuery = ".show materialized-views | where SourceTable == '" + tableName + "' | count";
        KustoQueryResponse kustoQueryResponse = service.executeQuery(databaseName, materializedViewQuery);
        if (kustoQueryResponse.isError()) {
            throw new ProcessException("Error occurred while checking if table is materialized view source");
        }
        KustoResultSetTable res = kustoQueryResponse.getIngestionResultSet();
        res.next();
        boolean isDestinationTableMaterializedViewSource = res.getLong(0) > 0;
        if (isDestinationTableMaterializedViewSource) {
            String tableEngineV3Query = ".show table " + tableName + " details | project todynamic(ShardingPolicy).UseShardEngine";
            KustoQueryResponse kustoQueryRes = service.executeQuery(databaseName, tableEngineV3Query);
            if (kustoQueryRes.isError()) {
                throw new ProcessException("Error occurred while checking if table is using V3 engine");
            }
            KustoResultSetTable resV3 = kustoQueryRes.getIngestionResultSet();
            resV3.next();
            return resV3.getBoolean(0);
        } else {
            return false;
        }
    }

    protected String executeMoveExtentsAsyncOperation(String databaseName, String moveExtentsQuery) throws ProcessException {
        KustoQueryResponse kustoQueryResponse = service.executeQuery(databaseName, moveExtentsQuery);
        if (kustoQueryResponse.isError()) {
            throw new ProcessException("Error occurred while moving extents");
        }
        KustoResultSetTable res = kustoQueryResponse.getIngestionResultSet();
        res.next();
        return res.getString(0);
    }

    protected String pollAndFindExtentMergeAsyncOperation(final String databaseName, final String showOperationsQuery, final String stateCol)
            throws ProcessException, ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<String> moveExtentfuture = new CompletableFuture<>();
        ScheduledExecutorService statusScheduler = Executors.newScheduledThreadPool(1);
        Runnable moveExtentTask = () -> {
            try {
                KustoQueryResponse kustoQueryResponse = service.executeQuery(databaseName, showOperationsQuery);
                if (kustoQueryResponse.isError()) {
                    throw new ProcessException("Error occurred while checking ingestion status");
                }
                KustoResultSetTable operationDetailsResTemp = kustoQueryResponse.getIngestionResultSet();
                operationDetailsResTemp.next();
                String operationStatus = operationDetailsResTemp.getString(stateCol);
                getLogger().info("Status of operation {} ", operationStatus);
                switch (operationStatus) {
                    case "Completed":
                        moveExtentfuture.complete("Completed");
                        break;
                    case "Failed":
                    case "PartiallySucceeded":
                    case "Canceled":
                    case "Throttled":
                    case "BadInput":
                    case "Abandoned":
                        moveExtentfuture.complete("Failed");
                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                moveExtentfuture.completeExceptionally(new ProcessException("Error occurred while checking ingestion status", e));
            }
        };
        statusScheduler.scheduleWithFixedDelay(moveExtentTask, 1, 2, TimeUnit.SECONDS);
        String completionStatus = moveExtentfuture.get(1800, TimeUnit.SECONDS);
        shutDownScheduler(statusScheduler);
        return completionStatus;
    }

}