package org.qcri.rheem.phoenix.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.jdbc.operators.JdbcFilterOperator;


/**
 * Phoenix implementation of the {@link FilterOperator}.
 */
public class PhoenixFilterOperator extends JdbcFilterOperator implements PhoenixExecutionOperator {

    /**
     * Creates a new instance.
     */
    public PhoenixFilterOperator(PredicateDescriptor<Record> predicateDescriptor) {
        super(predicateDescriptor);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public PhoenixFilterOperator(FilterOperator<Record> that) {
        super(that);
    }

    @Override
    protected PhoenixFilterOperator createCopy() {
        return new PhoenixFilterOperator(this);
    }
}
