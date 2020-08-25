package org.qcri.rheem.jdbc.operators;

import org.qcri.rheem.basic.operators.TableSink;
import org.qcri.rheem.jdbc.compiler.FunctionCompiler;

import java.sql.Connection;

/**
 * SQL implementation for the {@link TableSink}.
 */
public abstract class JdbcTableSink extends TableSink implements JdbcExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @see TableSink#TableSink(String, String...)
     */
    public JdbcTableSink(String tableName, String... columnNames) {
        super(tableName, columnNames);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JdbcTableSink(JdbcTableSink that) {
        super(that);
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        return this.getTableName();
    }
}
