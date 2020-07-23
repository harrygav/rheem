package org.qcri.rheem.hbase.channels;

import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.optimizer.channels.DefaultChannelConversion;
import org.qcri.rheem.hbase.operators.HBaseToStreamOperator;
import org.qcri.rheem.hbase.platform.HBasePlatform;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.platform.JavaPlatform;

import java.util.Arrays;
import java.util.Collection;

/**
 * {@link ChannelConversion}s for the {@link JavaPlatform}.
 */
public class ChannelConversions {

    public static final ChannelConversion HBASE_TO_STREAM_CONVERSION = new DefaultChannelConversion(
            HBasePlatform.getInstance().getHbaseQueryChannelDescriptor(),
            StreamChannel.DESCRIPTOR,
            () -> new HBaseToStreamOperator(HBasePlatform.getInstance())
    );

    public static Collection<ChannelConversion> ALL = Arrays.asList(
            HBASE_TO_STREAM_CONVERSION
/*            STREAM_TO_HDFS_OBJECT_FILE,
            COLLECTION_TO_HDFS_OBJECT_FILE,
            HDFS_OBJECT_FILE_TO_STREAM,
            HDFS_TSV_TO_STREAM,
            STREAM_TO_HDFS_TSV,
            COLLECTION_TO_HDFS_TSV*/
    );
}
