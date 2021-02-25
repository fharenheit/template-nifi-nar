package io.datadynamics.bigdata.nifi.processors.sample;

import io.datadynamics.bigdata.nifi.processors.sample.csv.CSVReader;
import io.datadynamics.bigdata.nifi.processors.sample.csv.CSVUtils;
import io.datadynamics.bigdata.nifi.processors.sample.text.FreeFormTextRecordSetWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static io.datadynamics.bigdata.nifi.processors.sample.UpdateRecord.RECORD_READER;
import static org.apache.nifi.processor.util.list.AbstractListProcessor.RECORD_WRITER;

public class TestUpdateRecord {

    private TestRunner runner;

    private CSVReader reader;

    private FreeFormTextRecordSetWriter writer;

    @Before
    public void setup() throws InitializationException {
        this.reader = new CSVReader();
        this.writer = new FreeFormTextRecordSetWriter();

        // 테스트할 Processor
        this.runner = TestRunners.newTestRunner(UpdateRecord.class);

        // Processor를 테스트하기 위해서 필요한 Reader, Writer를 생성하고 설정
        this.runner.addControllerService("reader", this.reader);
        this.runner.setProperty(this.reader, CSVUtils.FIRST_LINE_IS_HEADER, "true");
        this.runner.setProperty(this.reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "infer-schema");
        this.runner.setProperty(this.reader, CSVUtils.VALUE_SEPARATOR, ",");
        this.runner.enableControllerService(this.reader);

        this.runner.addControllerService("writer", this.writer);
        this.runner.setProperty(this.writer, "Text", "1");
        this.runner.enableControllerService(this.writer);

        this.runner.setProperty(RECORD_READER, "reader");
        this.runner.setProperty(RECORD_WRITER, "writer");
    }

    @Test
    public void csvReader() {
        runner.enqueue("C1,C2,C3,C4,C5,C6");
        runner.enqueue("1,2,3,4,a|b|c|d|e,5");
        runner.enqueue("1,2,3,4,a|b|c|d|e,5");
        runner.enqueue("1,2,3,4,a|b|c|d|e,5");
        runner.enqueue("1,2,3,4,a|b|c|d|e,5");
        runner.enqueue("1,2,3,4,a|b|c|d|e,5");
        runner.enqueue("1,2,3,4,a|b|c|d|e,5");
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0);
        System.out.println(new String(out.getData()));
    }

}
