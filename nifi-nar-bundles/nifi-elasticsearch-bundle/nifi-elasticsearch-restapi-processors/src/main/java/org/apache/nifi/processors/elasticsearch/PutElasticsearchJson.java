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

package org.apache.nifi.processors.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.ElasticsearchException;
import org.apache.nifi.elasticsearch.IndexOperationRequest;
import org.apache.nifi.elasticsearch.IndexOperationResponse;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"json", "elasticsearch", "elasticsearch5", "elasticsearch6", "elasticsearch7", "elasticsearch8", "put", "index"})
@CapabilityDescription("An Elasticsearch put processor that uses the official Elastic REST client libraries.")
@WritesAttributes({
        @WritesAttribute(attribute = "elasticsearch.put.error",
                description = "The error message if there is an issue parsing the FlowFile, sending the parsed document to Elasticsearch or parsing the Elasticsearch response"),
        @WritesAttribute(attribute = "elasticsearch.bulk.error", description = "The _bulk response if there was an error during processing the document within Elasticsearch.")
})
@DynamicProperties({
        @DynamicProperty(
                name = "The name of the Bulk request header",
                value = "The value of the Bulk request header",
                expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                description = "Prefix: " + AbstractPutElasticsearch.BULK_HEADER_PREFIX +
                        " - adds the specified property name/value as a Bulk request header in the Elasticsearch Bulk API body used for processing. " +
                        "These parameters will override any matching parameters in the _bulk request body."),
        @DynamicProperty(
                name = "The name of a URL query parameter to add",
                value = "The value of the URL query parameter",
                expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                description = "Adds the specified property name/value as a query parameter in the Elasticsearch URL used for processing. " +
                        "These parameters will override any matching parameters in the _bulk request body. " +
                        "If FlowFiles are batched, only the first FlowFile in the batch is used to evaluate property values.")
})
@SystemResourceConsideration(
        resource = SystemResource.MEMORY,
        description = "The Batch of FlowFiles will be stored in memory until the bulk operation is performed.")
public class PutElasticsearchJson extends AbstractPutElasticsearch {
    static final Relationship REL_FAILED_DOCUMENTS = new Relationship.Builder()
            .name("errors").description("If a \"Output Error Documents\" is set, any FlowFile(s) corresponding to Elasticsearch document(s) " +
                            "that resulted in an \"error\" (within Elasticsearch) will be routed here.")
            .autoTerminateDefault(true).build();

    static final PropertyDescriptor ID_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("put-es-json-id-attr")
            .displayName("Identifier Attribute")
            .description("The name of the FlowFile attribute containing the identifier for the document. If the Index Operation is \"index\", "
                    + "this property may be left empty or evaluate to an empty value, in which case the document's identifier will be "
                    + "auto-generated by Elasticsearch. For all other Index Operations, the attribute must evaluate to a non-empty value.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .build();

    static final PropertyDescriptor SCRIPT = new PropertyDescriptor.Builder()
            .name("put-es-json-script")
            .displayName("Script")
            .description("The script for the document update/upsert. Only applies to Update/Upsert operations. " +
                    "Must be parsable as JSON Object. " +
                    "If left blank, the FlowFile content will be used for document update/upsert")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor SCRIPTED_UPSERT = new PropertyDescriptor.Builder()
            .name("put-es-json-scripted-upsert")
            .displayName("Scripted Upsert")
            .description("Whether to add the scripted_upsert flag to the Upsert Operation. " +
                    "Forces Elasticsearch to execute the Script whether or not the document exists, defaults to false. " +
                    "If the Upsert Document provided (from FlowFile content) will be empty, but sure to set the " +
                    CLIENT_SERVICE.getDisplayName() + " controller service's " + ElasticSearchClientService.SUPPRESS_NULLS.getDisplayName() +
                    " to " + ElasticSearchClientService.NEVER_SUPPRESS.getDisplayName() + " or no \"upsert\" doc will be, " +
                    "included in the request to Elasticsearch and the operation will not create a new document for the script " +
                    "to execute against, resulting in a \"not_found\" error")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor DYNAMIC_TEMPLATES = new PropertyDescriptor.Builder()
            .name("put-es-json-dynamic_templates")
            .displayName("Dynamic Templates")
            .description("The dynamic_templates for the document. Must be parsable as a JSON Object. " +
                    "Requires Elasticsearch 7+")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
        .name("put-es-json-charset")
        .displayName("Character Set")
        .description("Specifies the character set of the document data.")
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue(StandardCharsets.UTF_8.name())
        .required(true)
        .build();

    static final PropertyDescriptor OUTPUT_ERROR_DOCUMENTS = new PropertyDescriptor.Builder()
        .name("put-es-json-error-documents")
        .displayName("Output Error Documents")
        .description("If this configuration property is true, the response from Elasticsearch will be examined for failed documents " +
                "and the FlowFile(s) associated with the failed document(s) will be sent to the \"" + REL_FAILED_DOCUMENTS.getName() + "\" relationship " +
                "with \"elasticsearch.bulk.error\" attributes.")
        .allowableValues("true", "false")
        .defaultValue("false")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(true)
        .build();

    static final PropertyDescriptor NOT_FOUND_IS_SUCCESSFUL = new PropertyDescriptor.Builder()
        .name("put-es-json-not_found-is-error")
        .displayName("Treat \"Not Found\" as Success")
        .description("If true, \"not_found\" Elasticsearch Document associated FlowFiles will be routed to the \"" + REL_SUCCESS.getName() +
                "\" relationship, otherwise to the \"" + REL_FAILED_DOCUMENTS.getName() + "\" relationship. " +
                "If " + OUTPUT_ERROR_RESPONSES.getDisplayName() + " is \"true\" then \"not_found\" responses from Elasticsearch " +
                "will be sent to the " + REL_ERROR_RESPONSES.getName() + " relationship")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(false)
        .dependsOn(OUTPUT_ERROR_DOCUMENTS, "true")
        .build();

    static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        ID_ATTRIBUTE, INDEX_OP, INDEX, TYPE, SCRIPT, SCRIPTED_UPSERT, DYNAMIC_TEMPLATES, BATCH_SIZE, CHARSET, CLIENT_SERVICE,
        LOG_ERROR_RESPONSES, OUTPUT_ERROR_RESPONSES, OUTPUT_ERROR_DOCUMENTS, NOT_FOUND_IS_SUCCESSFUL
    ));
    static final Set<Relationship> BASE_RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        REL_SUCCESS, REL_FAILURE, REL_RETRY, REL_FAILED_DOCUMENTS
    )));

    private boolean outputErrors;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    Set<Relationship> getBaseRelationships() {
        return BASE_RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);

        this.outputErrors = context.getProperty(OUTPUT_ERROR_DOCUMENTS).asBoolean();
        this.notFoundIsSuccessful = context.getProperty(NOT_FOUND_IS_SUCCESSFUL).asBoolean();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final int batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();

        final List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles.isEmpty()) {
            return;
        }

        final String idAttribute = context.getProperty(ID_ATTRIBUTE).getValue();

        final List<FlowFile> originals = new ArrayList<>(flowFiles.size());
        final List<IndexOperationRequest> operations = new ArrayList<>(flowFiles.size());

        for (final FlowFile input : flowFiles) {
            addOperation(operations, originals, idAttribute, context, session, input);
        }

        if (!originals.isEmpty()) {
            try {
                final List<FlowFile> errorDocuments = indexDocuments(operations, originals, context, session);
                handleResponse(context, session, errorDocuments, originals);
            } catch (final ElasticsearchException ese) {
                final String msg = String.format("Encountered a server-side problem with Elasticsearch. %s",
                        ese.isElastic() ? "Routing to retry." : "Routing to failure");
                getLogger().error(msg, ese);
                final Relationship rel = ese.isElastic() ? REL_RETRY : REL_FAILURE;
                transferFlowFilesOnException(ese, rel, session, true, originals.toArray(new FlowFile[0]));
            } catch (final JsonProcessingException jpe) {
                getLogger().warn("Could not log Elasticsearch operation errors nor determine which documents errored.", jpe);
                final Relationship rel = outputErrors ? REL_FAILED_DOCUMENTS : REL_FAILURE;
                transferFlowFilesOnException(jpe, rel, session, true, originals.toArray(new FlowFile[0]));
            } catch (final Exception ex) {
                getLogger().error("Could not index documents.", ex);
                transferFlowFilesOnException(ex, REL_FAILURE, session, false, originals.toArray(new FlowFile[0]));
                context.yield();
            }
        } else {
            getLogger().warn("No FlowFiles successfully parsed for sending to Elasticsearch");
        }
    }

    private void addOperation(final List<IndexOperationRequest> operations, final List<FlowFile> originals, final String idAttribute,
                              final ProcessContext context, final ProcessSession session, FlowFile input) {
        final String indexOp = context.getProperty(INDEX_OP).evaluateAttributeExpressions(input).getValue();
        final String index = context.getProperty(INDEX).evaluateAttributeExpressions(input).getValue();
        final String type = context.getProperty(TYPE).evaluateAttributeExpressions(input).getValue();
        final String id = StringUtils.isNotBlank(idAttribute) && StringUtils.isNotBlank(input.getAttribute(idAttribute)) ? input.getAttribute(idAttribute) : null;

        final Map<String, Object> scriptMap = getMapFromAttribute(SCRIPT, context, input);
        final boolean scriptedUpsert = context.getProperty(SCRIPTED_UPSERT).evaluateAttributeExpressions(input).asBoolean();
        final Map<String, Object> dynamicTemplatesMap = getMapFromAttribute(DYNAMIC_TEMPLATES, context, input);

        final Map<String, String> dynamicProperties = getDynamicProperties(context, input);
        final Map<String, String> bulkHeaderFields = getBulkHeaderParameters(dynamicProperties);

        final String charset = context.getProperty(CHARSET).evaluateAttributeExpressions(input).getValue();

        try (final InputStream inStream = session.read(input)) {
            final byte[] result = IOUtils.toByteArray(inStream);
            @SuppressWarnings("unchecked")
            final Map<String, Object> contentMap = objectMapper.readValue(new String(result, charset), Map.class);

            final IndexOperationRequest.Operation o = IndexOperationRequest.Operation.forValue(indexOp);
            operations.add(new IndexOperationRequest(index, type, id, contentMap, o, scriptMap, scriptedUpsert, dynamicTemplatesMap, bulkHeaderFields));

            originals.add(input);
        } catch (final IOException ioe) {
            getLogger().error("Could not read FlowFile content valid JSON.", ioe);
            input = session.putAttribute(input, "elasticsearch.put.error", ioe.getMessage());
            session.penalize(input);
            session.transfer(input, REL_FAILURE);
        } catch (final Exception ex) {
            getLogger().error("Could not index documents.", ex);
            input = session.putAttribute(input, "elasticsearch.put.error", ex.getMessage());
            session.penalize(input);
            session.transfer(input, REL_FAILURE);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getMapFromAttribute(final PropertyDescriptor propertyDescriptor, final ProcessContext context, final FlowFile input) {
        final String dynamicTemplates = context.getProperty(propertyDescriptor).evaluateAttributeExpressions(input).getValue();
        try {
            return StringUtils.isNotBlank(dynamicTemplates) ? MAPPER.readValue(dynamicTemplates, Map.class) : Collections.emptyMap();
        } catch (final JsonProcessingException jpe) {
            throw new ProcessException(propertyDescriptor.getDisplayName() + " must be a String parsable into a JSON Object", jpe);
        }
    }

    private List<FlowFile> indexDocuments(final List<IndexOperationRequest> operations, final List<FlowFile> originals, final ProcessContext context, final ProcessSession session) throws IOException {
        final Map<String, String> dynamicProperties = getDynamicProperties(context, originals.get(0));
        final IndexOperationResponse response = clientService.get().bulk(operations, getRequestURLParameters(dynamicProperties));

        final Map<Integer, Map<String, Object>> errors = findElasticsearchResponseErrors(response);
        final List<FlowFile> errorDocuments = outputErrors ? new ArrayList<>(errors.size()) : Collections.emptyList();
        if (outputErrors) {
            errors.forEach((index, error) -> {
                String errorMessage;
                try {
                    errorMessage = objectMapper.writeValueAsString(error);
                } catch (JsonProcessingException e) {
                    errorMessage = String.format(
                            "{\"error\": {\"type\": \"elasticsearch_response_parse_error\", \"reason\": \"%s\"}}",
                            e.getMessage().replace("\"", "\\\"")
                    );
                }
                errorDocuments.add(session.putAttribute(originals.get(index), "elasticsearch.bulk.error", errorMessage));
            });
        }

        if (!errors.isEmpty()) {
            handleElasticsearchDocumentErrors(errors, session, null);
        }

        return errorDocuments;
    }

    private void handleResponse(final ProcessContext context, final ProcessSession session, final List<FlowFile> errorDocuments, final List<FlowFile> originals) {
        session.transfer(errorDocuments, REL_FAILED_DOCUMENTS);
        errorDocuments.forEach(e ->
                session.getProvenanceReporter().send(
                        e,
                        clientService.get().getTransitUrl(
                                context.getProperty(INDEX).evaluateAttributeExpressions(e).getValue(),
                                context.getProperty(TYPE).evaluateAttributeExpressions(e).getValue()
                        ),
                        "Elasticsearch _bulk operation error"
                )
        );

        final List<FlowFile> successfulDocuments = originals.stream().filter(f -> !errorDocuments.contains(f)).collect(Collectors.toList());
        session.transfer(successfulDocuments, REL_SUCCESS);
        successfulDocuments.forEach(s ->
                session.getProvenanceReporter().send(
                        s,
                        clientService.get().getTransitUrl(
                                context.getProperty(INDEX).evaluateAttributeExpressions(s).getValue(),
                                context.getProperty(TYPE).evaluateAttributeExpressions(s).getValue()
                        )
                )
        );
    }
}
