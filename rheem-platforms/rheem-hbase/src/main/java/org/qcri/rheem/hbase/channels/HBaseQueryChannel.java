package org.qcri.rheem.hbase.channels;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.AbstractChannelInstance;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.hbase.platform.HBasePlatform;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * {@link Channel} between two {@link JavaExecutionOperator}s using an intermediate {@link Collection}.
 */
public class HBaseQueryChannel extends Channel {

    public HBaseQueryChannel(ChannelDescriptor descriptor, OutputSlot<?> producerSlot) {
        super(descriptor, producerSlot);
    }

    public HBaseQueryChannel(HBaseQueryChannel parent) {
        super(parent);
    }

    @Override
    public Channel copy() {
        return new HBaseQueryChannel(this);
    }

    @Override
    public HBaseQueryChannel.Instance createInstance(Executor executor, OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
        return new Instance(executor, producerOperatorContext, producerOutputIndex);
    }


    public class Instance extends AbstractChannelInstance {

        private ArrayList<String> projectedFields = null;
        private Scan hbaseScan = null;
        private Table table = null;

        public ArrayList<String> getProjectedFields() {
            return projectedFields;
        }

        public void setProjectedFields(ArrayList<String> projectedFields) {
            this.projectedFields = projectedFields;
        }


        public Table getTable() {
            return table;
        }

        public void setTable(Table table) {
            this.table = table;
        }


        public Scan getHbaseScan() {
            return hbaseScan;
        }

        public void setHbaseScan(Scan hbaseScan) {
            this.hbaseScan = hbaseScan;
        }

        /**
         * Creates a new instance and registers it with its {@link Executor}.
         *
         * @param executor                that maintains this instance
         * @param producerOperatorContext the {@link OptimizationContext.OperatorContext} for the producing
         *                                {@link ExecutionOperator}
         * @param producerOutputIndex     the output index of the producer {@link ExecutionTask}
         */
        protected Instance(Executor executor, OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
            super(executor, producerOperatorContext, producerOutputIndex);
        }

        @Override
        public Channel getChannel() {
            return HBaseQueryChannel.this;
        }

        @Override
        protected void doDispose() throws Throwable {

        }
    }

    public static class Descriptor extends ChannelDescriptor {

        private final HBasePlatform platform;

        public Descriptor(HBasePlatform platform) {
            super(HBaseQueryChannel.class, false, false);
            this.platform = platform;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Descriptor that = (Descriptor) o;
            return Objects.equals(platform, that.platform);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), platform);
        }
    }
}

