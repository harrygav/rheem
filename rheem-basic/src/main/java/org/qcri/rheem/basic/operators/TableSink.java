package org.qcri.rheem.basic.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.types.RecordType;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.types.DataSetType;

/**
 * {@link UnarySink} that writes Records to a database table.
 */
public class TableSink extends UnarySink<Record> {

    private final String tableName;

    public String getTableName() {
        return tableName;
    }

    /**
     * Creates a new instance.
     *
     * @param tableName   name of the table to be written
     * @param columnNames names of the columns in the tables; can be omitted but allows to inject schema information
     *                    into Rheem, so as to allow specific optimizations
     */
    public TableSink(String tableName, String... columnNames) {
        this(tableName, createOutputDataSetType(columnNames));
    }

    public TableSink(String tableName, DataSetType<Record> type) {
        super(type);
        this.tableName = tableName;
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

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public TableSink(TableSink that) {
        super(that);
        this.tableName = that.getTableName();
    }

}
