package io.datadynamics.bigdata.nifi.processors.sample.convert;

import org.apache.nifi.mock.MockProcessContext;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestConvertRecord {

    @Test
    public void convert() {
        ConvertRecord convertRecord = new ConvertRecord();

        MockFlowFile mockFlowFile = new MockFlowFile(1);
        MockProcessContext mockProcessContext = new MockProcessContext();
        MapRecord record = new MapRecord(schema(), values());

        Record convertedRecord = convertRecord.process(record, mockFlowFile, mockProcessContext, 1);
        System.out.println(convertedRecord);
    }

    Map values() {
        Map map = new HashMap();
        map.put("C1", "a");
        map.put("C5", "1|2|3|4|5");
        return map;
    }

    SimpleRecordSchema schema() {
        List fields = new ArrayList();
        fields.add(new RecordField("C1", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("C5", RecordFieldType.STRING.getDataType()));
        return new SimpleRecordSchema(fields);
    }

}
