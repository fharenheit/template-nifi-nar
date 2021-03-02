package org.apache.nifi.serialization;

import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.util.List;

public class ExtendedSimpleRecordSchema extends SimpleRecordSchema {


    public ExtendedSimpleRecordSchema(List<RecordField> fields) {
        super(fields);
    }

    public ExtendedSimpleRecordSchema(List<RecordField> fields, SchemaIdentifier id) {
        super(fields, id);
    }

    public ExtendedSimpleRecordSchema(String text, String schemaFormat, SchemaIdentifier id) {
        super(text, schemaFormat, id);
    }

    public ExtendedSimpleRecordSchema(SchemaIdentifier id) {
        super(id);
    }

    public ExtendedSimpleRecordSchema(List<RecordField> fields, String text, String schemaFormat, SchemaIdentifier id) {
        super(fields, text, schemaFormat, id);
    }

}
