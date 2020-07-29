package org.qcri.rheem.hive.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.jdbc.operators.JdbcProjectionOperator;

/**
 * Apache Hive implementation of the {@link FilterOperator}.
 */
public class HiveProjectionOperator extends JdbcProjectionOperator implements org.qcri.rheem.hive.operators.HiveExecutionOperator {

    public HiveProjectionOperator(String... fieldNames) {
        super(fieldNames);
    }

    public HiveProjectionOperator(ProjectionDescriptor<Record, Record> functionDescriptor) {
        super(functionDescriptor);
    }

    public HiveProjectionOperator(MapOperator<Record, Record> that) {
        super(that);
    }

    @Override
    protected HiveProjectionOperator createCopy() {
        return new HiveProjectionOperator(this);
    }

}
