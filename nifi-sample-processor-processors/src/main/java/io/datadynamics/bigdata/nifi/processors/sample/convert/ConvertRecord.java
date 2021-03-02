package io.datadynamics.bigdata.nifi.processors.sample.convert;

import io.datadynamics.bigdata.nifi.processors.sample.csv.AbstractRecordProcessor;
import org.apache.avro.Schema;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@EventDriven
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"example"})
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
        @WritesAttribute(attribute = "record.count", description = "The number of records in the FlowFile"),
        @WritesAttribute(attribute = "record.error.message", description = "This attribute provides on failure the error message encountered by the Reader or Writer.")
})
@CapabilityDescription("설정한 Record Reader와 Record Writer Controller Service를 통해 파일의 포맷을 변경합니다. "
        + "The Reader and Writer must be configured with \"matching\" schemas. By this, we mean the schemas must have the same field names. The types of the fields "
        + "do not have to be the same if a field value can be coerced from one type to another. For instance, if the input schema has a field named \"balance\" of type double, "
        + "the output schema can have a field named \"balance\" with a type of string, double, or float. If any field is present in the input that is not present in the output, "
        + "the field will be left out of the output. If any field is specified in the output schema but is not present in the input data/schema, then the field will not be "
        + "present in the output or will have a null value, depending on the writer.")
public class ConvertRecord extends AbstractRecordProcessor {

    Logger logger = LoggerFactory.getLogger(ConvertRecord.class);

    Schema.Parser parser = new Schema.Parser();

    public static final PropertyDescriptor ARRAY_COLUMN_NAMES = new PropertyDescriptor.Builder()
            .name("array-column-names")
            .displayName("배열 컬럼명 목록 (구분자 : 콤마)")
            .description("배열 컬럼을 지정하기 위한 컬럼명 목록")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .defaultValue("")
            .required(true)
            .build();

    public static final PropertyDescriptor ARRAY_COLUMN_NAMES_SEPARATOR = new PropertyDescriptor.Builder()
            .name("array-column-names-separator")
            .displayName("실제 값을 배열로 만들기 위한 값 구분자")
            .description("배열로 구성하기 위해서 다수의 값을 배열로 만들기 위한 문자열 구분자")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .defaultValue("|")
            .required(true)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(INCLUDE_ZERO_RECORD_FLOWFILES);
        properties.add(ARRAY_COLUMN_NAMES);
        properties.add(ARRAY_COLUMN_NAMES_SEPARATOR);
        return properties;
    }

    @Override
    protected Record process(final Record record, final FlowFile flowFile, final ProcessContext context, final long count) {
//        String separator = context.getProperty(ARRAY_COLUMN_NAMES_SEPARATOR).getValue();
//        String columnNames = context.getProperty(ARRAY_COLUMN_NAMES).getValue();

        Schema avroSchema = parser.parse("{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"example2\",\n" +
                "  \"namespace\": \"io.datadynamics\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"C1\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"C5\",\n" +
                "      \"type\": {\n" +
                "        \"type\": \"array\",\n" +
                "        \"items\": \"string\"\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}");

        Map<String, Object> values = new HashMap();
        for (String name : record.getRawFieldNames()) {
            values.put(name, record.getValue(name));
        }
        values.put("C5", record.getAsString("C5").split("|"));

        MapRecord convertedRecord = new MapRecord(AvroTypeUtil.createSchema(avroSchema), values);
        return convertedRecord;
    }

    List names() {
        List list = new ArrayList();
        list.add("C5");
        return list;
    }

    SimpleRecordSchema schema(Record record, List<String> names) {
        List fields = new ArrayList();
        List<String> fieldNames = record.getSchema().getFieldNames();
        for (String fieldName : fieldNames) {
            Optional<DataType> dataType = record.getSchema().getDataType(fieldName);
            if (names.contains(fieldName)) {
                fields.add(new RecordField(fieldName, RecordFieldType.ARRAY.getDataType()));
            } else {
                fields.add(new RecordField(fieldName, dataType.get()));
            }
        }
        return new SimpleRecordSchema(fields);
    }

}
