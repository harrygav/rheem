package org.qcri.rheem.hbase.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;


/**
 * HBase implementation of the {@link FilterOperator}.
 */
public class HBaseFilterOperator<InputType> extends FilterOperator<InputType> implements HBaseExecutionOperator {

    public HBaseFilterOperator(PredicateDescriptor<InputType> predicateDescriptor) {
        super(predicateDescriptor);
    }

    public HBaseFilterOperator(FilterOperator<InputType> that) {
        super(that);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new HBaseFilterOperator(this);
    }
}
