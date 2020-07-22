package org.qcri.rheem.hbase.operators;

import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.hbase.platform.HBasePlatform;

import java.util.Collections;
import java.util.List;

public interface HBaseExecutionOperator extends ExecutionOperator {

    @Override
    default HBasePlatform getPlatform() {
        return HBasePlatform.getInstance();
    }

    @Override
    default List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(this.getPlatform().getHbaseQueryChannelDescriptor());
    }

    @Override
    default List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(this.getPlatform().getHbaseQueryChannelDescriptor());
    }
}