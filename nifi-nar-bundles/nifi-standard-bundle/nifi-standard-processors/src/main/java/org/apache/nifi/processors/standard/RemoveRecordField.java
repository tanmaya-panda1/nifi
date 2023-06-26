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

package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.record.path.RecordFieldRemover;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.serialization.record.Record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"update", "record", "generic", "schema", "json", "csv", "avro", "freeform", "text", "remove", "delete"})
@CapabilityDescription("Modifies the contents of a FlowFile that contains Record-oriented data (i.e. data that can be read via a RecordReader and written by a RecordWriter) "
        + "by removing selected fields. This Processor requires that at least one user-defined Property be added. "
        + "The name of the property is ignored by the processor, but could be a meaningful identifier for the user. "
        + "The value of the property should indicate a RecordPath that determines the field to be removed. "
        + "The processor executes the removal in the order in which these properties are added to the processor. "
        + "Set the \"Record Writer\" to \"Inherit Record Schema\" in order to use the updated Record Schema modified when removing Fields.")
@WritesAttributes({
        @WritesAttribute(attribute = "record.error.message", description = "This attribute provides on failure the error message encountered by the Reader or Writer.")
})
@DynamicProperty(name = "(Ignored)", value = "A RecordPath to the field to be removed.",
        description = "Allows users to specify fields to remove that match the RecordPath.",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@SeeAlso({UpdateRecord.class})
public class RemoveRecordField extends AbstractRecordProcessor {
    private volatile RecordPathCache recordPathCache;
    private volatile List<RecordFieldRemover.RecordPathRemovalProperties> recordPathsToRemove;

    private static final String ROOT_PATH = "/";

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .displayName(propertyDescriptorName)
                .description("The RecordPath to the field that needs to be removed for " + propertyDescriptorName)
                .required(false)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .addValidator(new RecordPathValidator())
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final boolean containsDynamic = validationContext.getProperties().keySet().stream().anyMatch(PropertyDescriptor::isDynamic);

        if (containsDynamic) {
            final List<ValidationResult> validationResults = new ArrayList<>(validationContext.getProperties().size());
            validationContext.getProperties().keySet().stream().filter(PropertyDescriptor::isDynamic)
                    .forEach(property -> {
                        final String path = validationContext.getProperty(property).evaluateAttributeExpressions().getValue();
                        if (ROOT_PATH.equals(path)) {
                            validationResults.add(new ValidationResult.Builder()
                                    .subject(property.getDisplayName()).valid(false)
                                    .explanation("the root RecordPath cannot be removed").build()
                            );
                        }
                    });
            return validationResults;
        }

        return Collections.singleton(new ValidationResult.Builder()
                .subject("User-defined Properties")
                .valid(false)
                .explanation("at least one RecordPath must be specified")
                .build());
    }

    @OnScheduled
    public void collectRecordPaths(final ProcessContext context) {
        recordPathCache = new RecordPathCache(context.getProperties().size() * 2);

        recordPathsToRemove = null;
    }

    @Override
    protected Record process(final Record record, final FlowFile flowFile, final ProcessContext context, final long count) {
        if (recordPathsToRemove == null) {
            recordPathsToRemove = new ArrayList<>(context.getProperties().size());
            context.getProperties().keySet().forEach(property -> {
                if (property.isDynamic()) {
                    // validate RecordPath from Expression Language (if applicable)
                    final String recordPath = context.getProperty(property).evaluateAttributeExpressions(flowFile).getValue();
                    if (ROOT_PATH.equals(recordPath)) {
                        throw new ProcessException(String.format("The root Record Path %s cannot be removed for %s", ROOT_PATH, property.getDisplayName()));
                    }
                    recordPathsToRemove.add(new RecordFieldRemover.RecordPathRemovalProperties(recordPath));
                }
            });
        }

        final RecordFieldRemover recordFieldRemover = new RecordFieldRemover(record, recordPathCache);
        recordPathsToRemove.forEach(recordFieldRemover::remove);
        return recordFieldRemover.getRecord();
    }
}
