package org.qcri.rheem.basic.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.types.RecordType;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Properties;

/**
 * {@link UnarySink} that writes Records to a database table.
 */
public class TableSink extends UnarySink<Record> {

    private String tableName;

    private String[] columnNames;

    private Properties props;

    /**
     * Creates a new instance.
     *
     * @param props        database connection properties
     * @param tableName    name of the table to be written
     * @param columnNames  names of the columns in the tables
     */
    public TableSink(Properties props, String tableName, String... columnNames) {
        this(props, tableName, columnNames, DataSetType.createDefault(Record.class));
    }

    public TableSink(Properties props, String tableName, String[] columnNames, DataSetType<Record> type) {
        super(type);
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.props = props;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public TableSink(TableSink that) {
        super(that);
        this.tableName = that.getTableName();
        this.props = that.getProperties();
    }

    public String getTableName() {
        return this.tableName;
    }

    protected void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public String[] getColumnNames() {
        return this.columnNames;
    }

    public Properties getProperties() {
        return this.props;
    }

    /**
     * Constructs an appropriate output {@link DataSetType} for the given column names.
     *
     * @param columnNames the column names or an empty array if unknown
     * @return the output {@link DataSetType}, which will be based upon a {@link RecordType} unless no {@code columnNames}
     * is empty
     */
    private static DataSetType<Record> createOutputDataSetType(String[] columnNames) {
        return columnNames.length == 0 ?
                DataSetType.createDefault(Record.class) :
                DataSetType.createDefault(new RecordType(columnNames));
    }
}
