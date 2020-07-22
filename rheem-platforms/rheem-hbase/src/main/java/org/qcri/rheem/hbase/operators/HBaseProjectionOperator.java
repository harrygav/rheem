package org.qcri.rheem.hbase.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.core.types.DataSetType;

/**
 * HBaseQL implementation of the {@link FilterOperator}.
 */
public class HBaseProjectionOperator extends MapOperator implements HBaseExecutionOperator {

    public HBaseProjectionOperator(String... fieldNames) {
        super(
                new ProjectionDescriptor<>(Record.class, Record.class, fieldNames),
                DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class)
        );
    }

    public HBaseProjectionOperator(ProjectionDescriptor<Record, Record> functionDescriptor) {
        super(functionDescriptor);
    }

    public HBaseProjectionOperator(MapOperator<Record, Record> that) {
        super(that);
    }

    @Override
    protected HBaseProjectionOperator createCopy() {
        return new HBaseProjectionOperator(this);
    }

}
