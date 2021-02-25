package io.datadynamics.bigdata.nifi.processors.sample.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.csv.*;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.*;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.stream.io.NonCloseableInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Tags({"example"})
@CapabilityDescription("CSV 형식의 데이터를 파싱해서 CSV 파일의 각 ROW를 별도의 레코드로 반환합니다. "
        + "이 Reader는 CSV 파일의 첫 라인을 읽어서 스키나를 추론하거나" +
        "헤더 라인이 있거나 또는 명시적으로 스키나를 제공할 수 있습니다. "
        + "자세한 내용은 Controller Service 문서를 참고하십시오.")
public class CSVReader extends SchemaRegistryService implements RecordReaderFactory {

    private static final AllowableValue HEADER_DERIVED = new AllowableValue("csv-header-derived", "헤더명과 헤더명의 자료형을 문자열로 사용",
            "첫번째 라인이 CSV 파일의 컬럼명으로 포함하는 헤더 라인이다. 컬럼명을 스키마로 사용하고 자료형은 모두 문자열입니다.");

    // CSV Parser
    public static final AllowableValue APACHE_COMMONS_CSV = new AllowableValue("commons-csv", "Apache Commons CSV", "Apache Commons CSV 기반 CSV Parser");
    public static final AllowableValue JACKSON_CSV = new AllowableValue("jackson-csv", "Jackson CSV", "Jackson Dataformats 기반 CSV Parser");


    public static final PropertyDescriptor CSV_PARSER = new PropertyDescriptor.Builder()
            .name("csv-reader-csv-parser")
            .displayName("CSV Parser")
            .description("CSV 파일을 읽을 때 사용하는 CSV Parser를 지정합니다. 참고: Parser간 서로 다른 기능을 제공하므로 성능 영향이 발생할 수 있습니다.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(APACHE_COMMONS_CSV, JACKSON_CSV)
            .defaultValue(APACHE_COMMONS_CSV.getValue())
            .required(true)
            .build();

    private volatile ConfigurationContext context;

    private volatile String csvParser;
    private volatile String dateFormat;
    private volatile String timeFormat;
    private volatile String timestampFormat;
    private volatile boolean firstLineIsHeader;
    private volatile boolean ignoreHeader;
    private volatile String charSet;

    // Dynamic CSV 포맷인 경우에만 초기화한다.
    private volatile CSVFormat csvFormat;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(CSV_PARSER);
        properties.add(DateTimeUtils.DATE_FORMAT);
        properties.add(DateTimeUtils.TIME_FORMAT);
        properties.add(DateTimeUtils.TIMESTAMP_FORMAT);
        properties.add(CSVUtils.CSV_FORMAT);
        properties.add(CSVUtils.VALUE_SEPARATOR);
        properties.add(CSVUtils.RECORD_SEPARATOR);
        properties.add(CSVUtils.FIRST_LINE_IS_HEADER);
        properties.add(CSVUtils.IGNORE_CSV_HEADER);
        properties.add(CSVUtils.QUOTE_CHAR);
        properties.add(CSVUtils.ESCAPE_CHAR);
        properties.add(CSVUtils.COMMENT_MARKER);
        properties.add(CSVUtils.NULL_STRING);
        properties.add(CSVUtils.TRIM_FIELDS);
        properties.add(CSVUtils.CHARSET);
        return properties;
    }

    @OnEnabled
    public void storeStaticProperties(final ConfigurationContext context) {
        this.context = context;

        this.csvParser = context.getProperty(CSV_PARSER).getValue();
        this.dateFormat = context.getProperty(DateTimeUtils.DATE_FORMAT).getValue();
        this.timeFormat = context.getProperty(DateTimeUtils.TIME_FORMAT).getValue();
        this.timestampFormat = context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue();
        this.firstLineIsHeader = context.getProperty(CSVUtils.FIRST_LINE_IS_HEADER).asBoolean();
        this.ignoreHeader = context.getProperty(CSVUtils.IGNORE_CSV_HEADER).asBoolean();
        this.charSet = context.getProperty(CSVUtils.CHARSET).getValue();

        // Ensure that if we are deriving schema from header that we always treat the first line as a header,
        // regardless of the 'First Line is Header' property
        final String accessStrategy = context.getProperty(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY).getValue();
        if (HEADER_DERIVED.getValue().equals(accessStrategy) || SchemaInferenceUtil.INFER_SCHEMA.getValue().equals(accessStrategy)) {
            this.firstLineIsHeader = true;
        }

        if (!CSVUtils.isDynamicCSVFormat(context)) {
            this.csvFormat = CSVUtils.createCSVFormat(context, Collections.emptyMap());
        } else {
            this.csvFormat = null;
        }
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger) throws IOException, SchemaNotFoundException {
        // Use Mark/Reset of a BufferedInputStream in case we read from the Input Stream for the header.
        in.mark(1024 * 1024);
        final RecordSchema schema = getSchema(variables, new NonCloseableInputStream(in), null);
        in.reset();

        CSVFormat csvFormat;
        if (this.csvFormat != null) {
            csvFormat = this.csvFormat;
        } else {
            csvFormat = CSVUtils.createCSVFormat(context, variables);
        }

        if (APACHE_COMMONS_CSV.getValue().equals(csvParser)) {
            return new CSVRecordReader(in, logger, schema, csvFormat, firstLineIsHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, charSet);
        } else if (JACKSON_CSV.getValue().equals(csvParser)) {
            return new JacksonCSVRecordReader(in, logger, schema, csvFormat, firstLineIsHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, charSet);
        } else {
            throw new IOException("Parser를 지원하지 않습니다");
        }
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String allowableValue, final SchemaRegistry schemaRegistry, final PropertyContext context) {
        if (allowableValue.equalsIgnoreCase(HEADER_DERIVED.getValue())) {
            return new CSVHeaderSchemaStrategy(context);
        } else if (allowableValue.equalsIgnoreCase(SchemaInferenceUtil.INFER_SCHEMA.getValue())) {
            final RecordSourceFactory<CSVRecordAndFieldNames> sourceFactory = (variables, in) -> new CSVRecordSource(in, context, variables);
            final SchemaInferenceEngine<CSVRecordAndFieldNames> inference = new CSVSchemaInference(new TimeValueInference(dateFormat, timeFormat, timestampFormat));
            return new InferSchemaAccessStrategy<>(sourceFactory, inference, getLogger());
        }

        return super.getSchemaAccessStrategy(allowableValue, schemaRegistry, context);
    }

    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>(super.getSchemaAccessStrategyValues());
        allowableValues.add(HEADER_DERIVED);
        allowableValues.add(SchemaInferenceUtil.INFER_SCHEMA);
        return allowableValues;
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return SchemaInferenceUtil.INFER_SCHEMA;
    }
}

