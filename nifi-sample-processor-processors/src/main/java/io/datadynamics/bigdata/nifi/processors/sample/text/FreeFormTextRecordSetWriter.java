package io.datadynamics.bigdata.nifi.processors.sample.text;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.InheritSchemaFromRecord;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SchemaRegistryRecordSetWriter;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Tags({"example"})
@CapabilityDescription("Writes the contents of a RecordSet as free-form text. The configured "
        + "text is able to make use of the Expression Language to reference each of the fields that are available "
        + "in a Record, as well as the attributes in the FlowFile and variables. If there is a name collision, the field name/value is used before attributes or variables. "
        + "Each record in the RecordSet will be separated by a single newline character.")
public class FreeFormTextRecordSetWriter extends SchemaRegistryRecordSetWriter implements RecordSetWriterFactory {
    static final PropertyDescriptor TEXT = new PropertyDescriptor.Builder()
            .name("Text")
            .description("The text to use when writing the results. This property will evaluate the Expression Language using any of the fields available in a Record.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();
    static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The Character set to use when writing the data to the FlowFile")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    private volatile PropertyValue textValue;

    private volatile Charset characterSet;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(TEXT);
        properties.add(CHARACTER_SET);
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        textValue = context.getProperty(TEXT);
        characterSet = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out, final Map<String, String> variables) {
        return new FreeFormTextWriter(textValue, characterSet, out, variables);
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(String strategy, SchemaRegistry schemaRegistry, PropertyContext context) {
        return new InheritSchemaFromRecord();
    }

}

