package org.qcri.rheem.phoenix.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.jdbc.operators.JdbcProjectionOperator;

/**
 * Phoenix implementation of the {@link FilterOperator}.
 */
public class PhoenixProjectionOperator extends JdbcProjectionOperator implements PhoenixExecutionOperator {

    public PhoenixProjectionOperator(String... fieldNames) {
        super(fieldNames);
    }

    public PhoenixProjectionOperator(ProjectionDescriptor<Record, Record> functionDescriptor) {
        super(functionDescriptor);
    }

    public PhoenixProjectionOperator(MapOperator<Record, Record> that) {
        super(that);
    }

    @Override
    protected PhoenixProjectionOperator createCopy() {
        return new PhoenixProjectionOperator(this);
    }

}
