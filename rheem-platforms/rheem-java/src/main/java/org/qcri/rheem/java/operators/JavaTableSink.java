package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.TableSink;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class JavaTableSink extends TableSink implements JavaExecutionOperator {
    public JavaTableSink(Properties props, String tableName, String... columnNames) {
        super(props, tableName, columnNames);
    }

    public JavaTableSink(Properties props, String tableName, DataSetType<Record> type) {
        super(props, tableName, type);
    }

    public JavaTableSink(TableSink that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, JavaExecutor javaExecutor, OptimizationContext.OperatorContext operatorContext) {
        return null;
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return null;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return null;
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return null;
    }
}
