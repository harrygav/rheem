package org.qcri.rheem.hbase.operators;

import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.platform.ChannelDescriptor;

import java.util.List;

/**
 * HBase implementation for the {@link TableSource}.
 */
public class HBaseTableSource extends TableSource implements HBaseExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @see TableSource#TableSource(String, String...)
     */
    public HBaseTableSource(String tableName, String... columnNames) {
        super(tableName, columnNames);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public HBaseTableSource(TableSource that) {
        super(that);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException("This operator has no input channels.");
    }
}
