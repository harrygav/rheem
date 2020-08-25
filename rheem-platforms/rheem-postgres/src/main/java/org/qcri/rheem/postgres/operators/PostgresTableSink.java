package org.qcri.rheem.postgres.operators;

import org.qcri.rheem.basic.operators.TableSink;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.jdbc.operators.JdbcTableSink;

import java.util.List;

/**
 * PostgreSQL implementation for the {@link TableSink}.
 */
public class PostgresTableSink extends JdbcTableSink implements PostgresExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @see TableSink#TableSink(String, String...)
     */
    public PostgresTableSink(String tableName, String... columnNames) {
        super(tableName, columnNames);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public PostgresTableSink(JdbcTableSink that) {
        super(that);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        throw new UnsupportedOperationException("This operator has no output channels.");
    }
}
