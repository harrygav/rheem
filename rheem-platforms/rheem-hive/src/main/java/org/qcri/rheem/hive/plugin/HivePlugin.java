package org.qcri.rheem.hive.plugin;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.hive.channels.ChannelConversions;
import org.qcri.rheem.hive.mapping.Mappings;
import org.qcri.rheem.hive.platform.HivePlatform;

import java.util.Arrays;
import java.util.Collection;

/**
 * This {@link Plugin} enables to use some basic Rheem {@link Operator}s on the {@link HivePlatform}.
 */
public class HivePlugin implements Plugin {

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Arrays.asList(HivePlatform.getInstance(), JavaPlatform.getInstance());
    }

    @Override
    public Collection<Mapping> getMappings() {
        return Mappings.ALL;
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return ChannelConversions.ALL;
    }

    @Override
    public void setProperties(Configuration configuration) {
        // Nothing to do, because we already configured the properties in #configureDefaults(...).
    }
}
