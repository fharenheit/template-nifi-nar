package io.datadynamics.bigdata.nifi.processors.sample;

import io.datadynamics.bigdata.nifi.processors.sample.csv.CSVReader;
import io.datadynamics.bigdata.nifi.processors.sample.csv.CSVUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.text.FreeFormTextRecordSetWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestUpdateRecord {

    private TestRunner runner;

    @BeforeClass
    public static void setUpSuite() {
    }

    @Before
    public void setup() throws InitializationException {
        CSVReader reader = new CSVReader();

        FreeFormTextRecordSetWriter writer = new FreeFormTextRecordSetWriter();

        runner = TestRunners.newTestRunner(UpdateRecord.class);
        runner.addControllerService("reader", reader);
        runner.setProperty(reader, CSVUtils.RECORD_SEPARATOR, "|");
        runner.enableControllerService(reader);

        runner.addControllerService("writer", writer);
        runner.setProperty(writer, "Text", "1");
        runner.enableControllerService(writer);

        PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
                .name("record-reader")
                .displayName("Record Reader")
                .description("The Record Reader to use for reading received messages." +
                        " The event hub name can be referred by Expression Language '${eventhub.name}' to access a schema.")
                .identifiesControllerService(RecordReaderFactory.class)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .required(false)
                .build();
        PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
                .name("record-writer")
                .displayName("Record Writer")
                .description("The Record Writer to use for serializing Records to an output FlowFile." +
                        " The event hub name can be referred by Expression Language '${eventhub.name}' to access a schema." +
                        " If not specified, each message will create a FlowFile.")
                .identifiesControllerService(RecordSetWriterFactory.class)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .required(false)
                .build();

        runner.setProperty(RECORD_READER, "reader");
        runner.setProperty(RECORD_WRITER, "writer");
    }

    @Test
    public void testLiteralReplacementValue() {
        runner.enqueue("1|2|3|4|a,b,c,d,e|5");
        runner.enqueue("1|2|3|4|a,b,c,d,e|5");
        runner.enqueue("1|2|3|4|a,b,c,d,e|5");
        runner.enqueue("1|2|3|4|a,b,c,d,e|5");
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0);
        System.out.println(out);
    }

}
