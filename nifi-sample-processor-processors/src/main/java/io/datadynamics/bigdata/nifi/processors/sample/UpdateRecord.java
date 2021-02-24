package io.datadynamics.bigdata.nifi.processors.sample;

import io.datadynamics.bigdata.nifi.processors.sample.csv.AbstractRecordProcessor;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.*;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathPropertyNameValidator;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.*;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"example"})
@SeeAlso({})
@CapabilityDescription("Updates the contents of a FlowFile that contains Record-oriented data (i.e., data that can be read via a RecordReader and written by a RecordWriter). "
        + "This Processor requires that at least one user-defined Property be added. The name of the Property should indicate a RecordPath that determines the field that should "
        + "be updated. The value of the Property is either a replacement value (optionally making use of the Expression Language) or is itself a RecordPath that extracts a value from "
        + "the Record. Whether the Property value is determined to be a RecordPath or a literal value depends on the configuration of the <Replacement Value Strategy> Property.")
@WritesAttributes({
        @WritesAttribute(attribute = "record.index", description =
                "This attribute provides the current row index and is only available inside the literal value expression."),
        @WritesAttribute(attribute = "record.error.message",
                description = "This attribute provides on failure the error message encountered by the Reader or Writer.")
})
public class UpdateRecord extends AbstractRecordProcessor {

    private static final String FIELD_NAME = "field.name";
    private static final String FIELD_VALUE = "field.value";
    private static final String FIELD_TYPE = "field.type";

    private static final String RECORD_INDEX = "record.index";

    private volatile RecordPathCache recordPathCache;

    private volatile List<String> recordPaths;

    static final AllowableValue LITERAL_VALUES = new AllowableValue("literal-value", "Literal 값",
            "속성에 입력 한 값 (표현식 언어가 평가 된 후)은 레코드 필드를 업데이트하는 데 사용할 원하는 값입니다. 표현식 언어는 'field.name', 'field.type'및 'field.value'변수를 참조하여 필드에 대한 정보와 평가중인 필드의 값에 액세스 할 수 있습니다.");

    static final AllowableValue RECORD_PATH_VALUES = new AllowableValue("record-path-value", "Record Path 값",
            "Property에 입력한 값 (Expression Language가 평가 된 후)은 사용할 리터럴 값이 아니라 레코드에 대해 평가되어야하는 레코드 경로이며 RecordPath의 결과는 레코드를 업데이트하는 데 사용됩니다. 이 옵션이 선택되고 레코드 경로가 주어진 레코드에 대해 여러 값을 생성하는 경우 입력 FlowFile은 '실패'관계로 라우팅됩니다.");

    static final PropertyDescriptor REPLACEMENT_VALUE_STRATEGY = new PropertyDescriptor.Builder()
            .name("replacement-value-strategy")
            .displayName("Replacement Value 전략")
            .description("설정한 replacement values를 어떻게 해석할이 여부를 지정합니다")
            .allowableValues(LITERAL_VALUES, RECORD_PATH_VALUES)
            .defaultValue(LITERAL_VALUES.getValue())
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after it has been converted to JSON")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be parsed as Avro or cannot be converted to JSON for any reason")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(REPLACEMENT_VALUE_STRATEGY);
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("RecordPath와 매핑하는 레코드 내에 필드를 교체하는데 사용할 값을 지정합니다 : " + propertyDescriptorName)
                .required(false)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .addValidator(new RecordPathPropertyNameValidator())
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        return Collections.emptyList();

/*
        final boolean containsDynamic = validationContext.getProperties().keySet().stream().anyMatch(PropertyDescriptor::isDynamic);

        if (containsDynamic) {
            return Collections.emptyList();
        }

        return Collections.singleton(new ValidationResult.Builder()
                .subject("User-defined Properties")
                .valid(false)
                .explanation("At least one RecordPath must be specified")
                .build());
*/
    }

    @OnScheduled
    public void createRecordPaths(final ProcessContext context) {
        recordPathCache = new RecordPathCache(context.getProperties().size() * 2);

        final List<String> recordPaths = new ArrayList<>(context.getProperties().size() - 2);
        for (final PropertyDescriptor property : context.getProperties().keySet()) {
            if (property.isDynamic()) {
                recordPaths.add(property.getName());
            }
        }

        this.recordPaths = recordPaths;
    }

    @Override
    protected Record process(Record record, final FlowFile flowFile, final ProcessContext context, final long count) {
        final boolean evaluateValueAsRecordPath = context.getProperty(REPLACEMENT_VALUE_STRATEGY).getValue().equals(RECORD_PATH_VALUES.getValue());

        for (final String recordPathText : recordPaths) {
            final RecordPath recordPath = recordPathCache.getCompiled(recordPathText);
            final RecordPathResult result = recordPath.evaluate(record);

            if (evaluateValueAsRecordPath) {
                final String replacementValue = context.getProperty(recordPathText).evaluateAttributeExpressions(flowFile).getValue();
                final RecordPath replacementRecordPath = recordPathCache.getCompiled(replacementValue);

                // If we have an Absolute RecordPath, we need to evaluate the RecordPath only once against the Record.
                // If the RecordPath is a Relative Path, then we have to evaluate it against each FieldValue.
                if (replacementRecordPath.isAbsolute()) {
                    record = processAbsolutePath(replacementRecordPath, result.getSelectedFields(), record);
                } else {
                    record = processRelativePath(replacementRecordPath, result.getSelectedFields(), record);
                }
            } else {
                final PropertyValue replacementValue = context.getProperty(recordPathText);

                if (replacementValue.isExpressionLanguagePresent()) {
                    final Map<String, String> fieldVariables = new HashMap<>();

                    result.getSelectedFields().forEach(fieldVal -> {
                        fieldVariables.clear();
                        fieldVariables.put(FIELD_NAME, fieldVal.getField().getFieldName());
                        fieldVariables.put(FIELD_VALUE, DataTypeUtils.toString(fieldVal.getValue(), (String) null));
                        fieldVariables.put(FIELD_TYPE, fieldVal.getField().getDataType().getFieldType().name());
                        fieldVariables.put(RECORD_INDEX, String.valueOf(count));

                        final String evaluatedReplacementVal = replacementValue.evaluateAttributeExpressions(flowFile, fieldVariables).getValue();
                        fieldVal.updateValue(evaluatedReplacementVal, RecordFieldType.STRING.getDataType());
                    });
                } else {
                    final String evaluatedReplacementVal = replacementValue.evaluateAttributeExpressions(flowFile).getValue();
                    result.getSelectedFields().forEach(fieldVal -> fieldVal.updateValue(evaluatedReplacementVal, RecordFieldType.STRING.getDataType()));
                }
            }
        }

        record.incorporateInactiveFields();

        return record;
    }

    private Record processAbsolutePath(final RecordPath replacementRecordPath, final Stream<FieldValue> destinationFields, final Record record) {
        final RecordPathResult replacementResult = replacementRecordPath.evaluate(record);
        final List<FieldValue> selectedFields = replacementResult.getSelectedFields().collect(Collectors.toList());
        final List<FieldValue> destinationFieldValues = destinationFields.collect(Collectors.toList());

        return updateRecord(destinationFieldValues, selectedFields, record);
    }

    private Record processRelativePath(final RecordPath replacementRecordPath, final Stream<FieldValue> destinationFields, Record record) {
        final List<FieldValue> destinationFieldValues = destinationFields.collect(Collectors.toList());

        for (final FieldValue fieldVal : destinationFieldValues) {
            final RecordPathResult replacementResult = replacementRecordPath.evaluate(record, fieldVal);
            final List<FieldValue> selectedFields = replacementResult.getSelectedFields().collect(Collectors.toList());
            final Object replacementObject = getReplacementObject(selectedFields);
            updateFieldValue(fieldVal, replacementObject);
        }

        return record;
    }

    private Record updateRecord(final List<FieldValue> destinationFields, final List<FieldValue> selectedFields, final Record record) {
        if (destinationFields.size() == 1 && !destinationFields.get(0).getParentRecord().isPresent()) {
            final Object replacement = getReplacementObject(selectedFields);
            if (replacement == null) {
                return record;
            }
            if (replacement instanceof Record) {
                return (Record) replacement;
            }

            final FieldValue replacementFieldValue = (FieldValue) replacement;
            if (replacementFieldValue.getValue() instanceof Record) {
                return (Record) replacementFieldValue.getValue();
            }

            final List<RecordField> fields = selectedFields.stream().map(FieldValue::getField).collect(Collectors.toList());
            final RecordSchema schema = new SimpleRecordSchema(fields);
            final Record mapRecord = new MapRecord(schema, new HashMap<>());
            for (final FieldValue selectedField : selectedFields) {
                mapRecord.setValue(selectedField.getField(), selectedField.getValue());
            }

            return mapRecord;
        } else {
            for (final FieldValue fieldVal : destinationFields) {
                final Object replacementObject = getReplacementObject(selectedFields);
                updateFieldValue(fieldVal, replacementObject);
            }
            return record;
        }
    }

    private void updateFieldValue(final FieldValue fieldValue, final Object replacement) {
        if (replacement instanceof FieldValue) {
            final FieldValue replacementFieldValue = (FieldValue) replacement;
            fieldValue.updateValue(replacementFieldValue.getValue(), replacementFieldValue.getField().getDataType());
        } else {
            fieldValue.updateValue(replacement);
        }
    }

    private Object getReplacementObject(final List<FieldValue> selectedFields) {
        if (selectedFields.size() > 1) {
            final List<RecordField> fields = selectedFields.stream().map(FieldValue::getField).collect(Collectors.toList());
            final RecordSchema schema = new SimpleRecordSchema(fields);
            final Record record = new MapRecord(schema, new HashMap<>());
            for (final FieldValue fieldVal : selectedFields) {
                record.setValue(fieldVal.getField(), fieldVal.getValue());
            }

            return record;
        }

        if (selectedFields.isEmpty()) {
            return null;
        } else {
            return selectedFields.get(0);
        }
    }
}
