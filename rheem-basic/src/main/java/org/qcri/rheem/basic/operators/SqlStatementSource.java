package org.qcri.rheem.basic.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.types.RecordType;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Properties;

/**
 * {@link UnarySource} that provides the tuples from a database table.
 */
public class SqlStatementSource extends UnarySource<Record> {

    private final String sqlStatement;
    private Properties props;

    public String getSqlStatement() {
        return sqlStatement;
    }

    public Properties getProperties() {
        return this.props;
    }

    public SqlStatementSource(String sqlStatement, Properties props, String... columnNames) {
        this(sqlStatement, props, createOutputDataSetType(columnNames));
    }

    public SqlStatementSource(String sqlStatement, Properties props, DataSetType<Record> type) {
        super(type);
        this.sqlStatement = sqlStatement;
        this.props = props;
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
    public SqlStatementSource(SqlStatementSource that) {
        super(that);
        this.sqlStatement = that.getSqlStatement();
        this.props = that.getProperties();
    }

}
