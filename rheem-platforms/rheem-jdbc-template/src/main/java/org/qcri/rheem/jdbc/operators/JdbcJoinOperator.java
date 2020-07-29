package org.qcri.rheem.jdbc.operators;

import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.jdbc.compiler.FunctionCompiler;

import java.sql.Connection;
import java.util.Optional;

/**
 * Template for JDBC-based {@link JoinOperator}.
 */
public abstract class JdbcJoinOperator<InputType0, InputType1, KeyType>
        extends JoinOperator<InputType0, InputType1, KeyType>
        implements JdbcExecutionOperator {

    public JdbcJoinOperator(DataSetType<InputType0> inputType0,
                            DataSetType<InputType1> inputType1,
                            TransformationDescriptor<InputType0, KeyType> keyDescriptor0,
                            TransformationDescriptor<InputType1, KeyType> keyDescriptor1) {
        super(keyDescriptor0, keyDescriptor1, inputType0, inputType1);
    }

    public JdbcJoinOperator(JoinOperator<InputType0, InputType1, KeyType> that) {
        super(that);
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        System.out.println(keyDescriptor0);
        System.out.println(keyDescriptor1);
        return "This is a join";
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.flink.join.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                JdbcExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.keyDescriptor0, configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.keyDescriptor1, configuration);
        return optEstimator;
    }
}
