package org.qcri.rheem.java.channels;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.AbstractChannelInstance;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.Collection;
import java.util.Properties;

/**
 * {@link Channel} between two {@link JavaExecutionOperator}s using an intermediate {@link Collection}.
 */
public class SqlStatementChannel extends Channel {

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(SqlStatementChannel.class, false, false);

    public SqlStatementChannel(ChannelDescriptor channelDescriptor, OutputSlot<?> outputSlot) {
        super(channelDescriptor, outputSlot);
        assert channelDescriptor == DESCRIPTOR;
    }

    private SqlStatementChannel(SqlStatementChannel parent) {
        super(parent);
    }

    @Override
    public SqlStatementChannel copy() {
        return new SqlStatementChannel(this);
    }

    @Override
    public Instance createInstance(Executor executor,
                                   OptimizationContext.OperatorContext producerOperatorContext,
                                   int producerOutputIndex) {
        return new Instance(executor, producerOperatorContext, producerOutputIndex);
    }

    /**
     * {@link JavaChannelInstance} implementation for the {@link SqlStatementChannel}.
     */
    public class Instance extends AbstractChannelInstance {

        private String sqlStatement;
        private Properties props;

        public Instance(Executor executor, OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
            super(executor, producerOperatorContext, producerOutputIndex);
        }

        public void accept(String sqlStatement, Properties props) {
            this.sqlStatement = sqlStatement;
            this.props = props;
            //this.setMeasuredCardinality(this.collection.size());
        }

        public String getSqlStatement() {
            return this.sqlStatement;
        }

        public Properties getProps() {
            return this.props;
        }

        @Override
        public Channel getChannel() {
            return SqlStatementChannel.this;
        }

        @Override
        protected void doDispose() {

        }

    }
}
