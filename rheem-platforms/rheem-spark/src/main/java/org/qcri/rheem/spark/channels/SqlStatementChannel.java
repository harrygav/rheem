package org.qcri.rheem.spark.channels;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.AbstractChannelInstance;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Executor;


/**
 *
 */
public class SqlStatementChannel extends Channel {

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(SqlStatementChannel.class, true, true);

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
     *
     */
    public class Instance extends AbstractChannelInstance {

        private String sqlStatement;

        public Instance(Executor executor, OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
            super(executor, producerOperatorContext, producerOutputIndex);
        }

        public void accept(String sqlStatement) {
            this.sqlStatement = sqlStatement;
            //this.setMeasuredCardinality(this.collection.size());
        }


        @Override
        public Channel getChannel() {
            return SqlStatementChannel.this;
        }

        @Override
        protected void doDispose() {
            logger.debug("Free {}.", this);
            this.sqlStatement = null;
        }

    }
}
