package org.qcri.rheem.hive.channels;

import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.optimizer.channels.DefaultChannelConversion;
import org.qcri.rheem.java.channels.SqlStatementChannel;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.jdbc.operators.SqlToSqlOperator;
import org.qcri.rheem.jdbc.operators.SqlToStreamOperator;
import org.qcri.rheem.hive.platform.HivePlatform;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Register for the {@link ChannelConversion}s supported for this platform.
 */
public class ChannelConversions {

    public static final ChannelConversion SQL_TO_STREAM_CONVERSION = new DefaultChannelConversion(
            HivePlatform.getInstance().getSqlQueryChannelDescriptor(),
            StreamChannel.DESCRIPTOR,
            () -> new SqlToStreamOperator(HivePlatform.getInstance())
    );

    public static final ChannelConversion SQL_TO_SQL_CONVERSION = new DefaultChannelConversion(
            HivePlatform.getInstance().getSqlQueryChannelDescriptor(),
            SqlStatementChannel.DESCRIPTOR,
            () -> new SqlToSqlOperator(HivePlatform.getInstance())
    );

    public static final Collection<ChannelConversion> ALL = Arrays.asList(
            SQL_TO_STREAM_CONVERSION,
            SQL_TO_SQL_CONVERSION
    );

}
