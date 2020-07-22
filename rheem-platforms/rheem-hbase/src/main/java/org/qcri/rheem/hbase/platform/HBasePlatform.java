package org.qcri.rheem.hbase.platform;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.TimeToCostConverter;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.hbase.channels.HBaseQueryChannel;
import org.qcri.rheem.hbase.execution.HBaseExecutor;

/**
 * {@link Platform} implementation for SQLite3.
 */
public class HBasePlatform extends Platform {


    private static final String PLATFORM_NAME = "HBase";

    private static final String CONFIG_NAME = "hbase";

    private static final String DEFAULT_CONFIG_FILE = "rheem-hbase-defaults.properties";

    private static HBasePlatform instance = null;

    public static HBasePlatform getInstance() {
        if (instance == null) {
            instance = new HBasePlatform();
        }
        return instance;
    }

    private HBasePlatform() {
        super(PLATFORM_NAME, CONFIG_NAME);
    }


    @Override
    protected void configureDefaults(Configuration configuration) {

    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new HBaseExecutor(this, job);
    }

    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        return null;
    }

    @Override
    public TimeToCostConverter createTimeToCostConverter(Configuration configuration) {
        return null;
    }

    private final HBaseQueryChannel.Descriptor hbaseQueryChannelDescriptor = new HBaseQueryChannel.Descriptor(this);


    public HBaseQueryChannel.Descriptor getHbaseQueryChannelDescriptor() {
        return this.hbaseQueryChannelDescriptor;
    }
}