package org.qcri.rheem.postgres.channels;

import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.optimizer.channels.DefaultChannelConversion;
import org.qcri.rheem.java.channels.SqlStatementChannel;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.jdbc.operators.SqlToSqlOperator;
import org.qcri.rheem.jdbc.operators.SqlToStreamOperator;
import org.qcri.rheem.postgres.platform.PostgresPlatform;

import java.util.Arrays;
import java.util.Collection;

/**
 * Register for the {@link ChannelConversion}s supported for this platform.
 */
public class ChannelConversions {

    public static final ChannelConversion SQL_TO_STREAM_CONVERSION = new DefaultChannelConversion(
            PostgresPlatform.getInstance().getSqlQueryChannelDescriptor(),
            StreamChannel.DESCRIPTOR,
            () -> new SqlToStreamOperator(PostgresPlatform.getInstance())
    );

    public static final ChannelConversion SQL_TO_SQL_CONVERSION = new DefaultChannelConversion(
            PostgresPlatform.getInstance().getSqlQueryChannelDescriptor(),
            SqlStatementChannel.DESCRIPTOR,
            () -> new SqlToSqlOperator(PostgresPlatform.getInstance())
    );

    public static final Collection<ChannelConversion> ALL = Arrays.asList(
            SQL_TO_SQL_CONVERSION
            //SQL_TO_STREAM_CONVERSION

    );

}
