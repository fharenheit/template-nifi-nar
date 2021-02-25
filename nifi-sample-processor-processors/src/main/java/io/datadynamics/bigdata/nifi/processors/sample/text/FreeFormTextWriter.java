package io.datadynamics.bigdata.nifi.processors.sample.text;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.*;

public class FreeFormTextWriter extends AbstractRecordSetWriter implements RecordSetWriter {

    private static final byte NEW_LINE = (byte) '\n';
    private final PropertyValue propertyValue;
    private final Charset charset;
    private final Map<String, String> variables;

    public FreeFormTextWriter(final PropertyValue textPropertyValue, final Charset characterSet, final OutputStream out, final Map<String, String> variables) {
        super(new BufferedOutputStream(out));
        this.propertyValue = textPropertyValue;
        this.charset = characterSet;
        this.variables = variables;
    }

    private List<String> getColumnNames(final RecordSchema schema) {
        final List<String> columnNames = new ArrayList<>();
        for (final RecordField field : schema.getFields()) {
            columnNames.add(field.getFieldName());
            for (final String alias : field.getAliases()) {
                columnNames.add(alias);
            }
        }

        return columnNames;
    }

    @Override
    public Map<String, String> writeRecord(final Record record) throws IOException {
        write(record, getOutputStream(), getColumnNames(record.getSchema()));
        return Collections.emptyMap();
    }

    private void write(final Record record, final OutputStream out, final List<String> columnNames) throws IOException {
        final int numCols = columnNames.size();
        final Map<String, String> values = new HashMap<>(numCols);
        for (int i = 0; i < numCols; i++) {
            final String columnName = columnNames.get(i);
            final String columnValue = record.getAsString(columnName);
            values.put(columnName, columnValue);
        }
        // Add attributes and variables (but don't override fields with the same name)
        for (Map.Entry<String, String> variable : variables.entrySet()) {
            values.putIfAbsent(variable.getKey(), variable.getValue());
        }

        final String evaluated = propertyValue.evaluateAttributeExpressions(values).getValue();
        out.write(evaluated.getBytes(charset));
        out.write(NEW_LINE);
    }

    @Override
    public String getMimeType() {
        return "text/plain";
    }
}

