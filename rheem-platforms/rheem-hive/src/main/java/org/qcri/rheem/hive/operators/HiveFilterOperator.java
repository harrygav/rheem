package org.qcri.rheem.hive.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.jdbc.operators.JdbcFilterOperator;


/**
 * PostgreSQL implementation of the {@link FilterOperator}.
 */
public class HiveFilterOperator extends JdbcFilterOperator implements org.qcri.rheem.hive.operators.HiveExecutionOperator {

    /**
     * Creates a new instance.
     */
    public HiveFilterOperator(PredicateDescriptor<Record> predicateDescriptor) {
        super(predicateDescriptor);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public HiveFilterOperator(FilterOperator<Record> that) {
        super(that);
    }

    @Override
    protected HiveFilterOperator createCopy() {
        return new HiveFilterOperator(this);
    }
}
