package org.qcri.rheem.hive.operators;

import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.jdbc.operators.JdbcJoinOperator;

/**
 * Apache Hive implementation of the {@link JoinOperator}.
 */
public class HiveJoinOperator<InputType0, InputType1, KeyType>
        extends JdbcJoinOperator<InputType0, InputType1, KeyType>
        implements HiveExecutionOperator {

    /**
     * Creates a new instance.
     */
    public HiveJoinOperator(DataSetType<InputType0> inputType0,
                            DataSetType<InputType1> inputType1,
                            TransformationDescriptor<InputType0, KeyType> keyDescriptor0,
                            TransformationDescriptor<InputType1, KeyType> keyDescriptor1) {
        super(inputType0, inputType1, keyDescriptor0, keyDescriptor1);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public HiveJoinOperator(JoinOperator<InputType0, InputType1, KeyType> that) {
        super(that);
    }

    @Override
    protected HiveJoinOperator<InputType0, InputType1, KeyType> createCopy() {
        return new HiveJoinOperator<InputType0, InputType1, KeyType>(this);
    }
}
