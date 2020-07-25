package org.qcri.rheem.hive;


import org.qcri.rheem.hive.platform.HivePlatform;
import org.qcri.rheem.hive.plugin.HiveConversionsPlugin;
import org.qcri.rheem.hive.plugin.HivePlugin;

/**
 * Register for relevant components of this module.
 */
public class Hive {

    private final static HivePlugin PLUGIN = new HivePlugin();

    private final static HiveConversionsPlugin CONVERSIONS_PLUGIN = new HiveConversionsPlugin();

    /**
     * Retrieve the {@link HivePlugin}.
     *
     * @return the {@link HivePlugin}
     */
    public static HivePlugin plugin() {
        return PLUGIN;
    }

    /**
     * Retrieve the {@link HiveConversionsPlugin}.
     *
     * @return the {@link HiveConversionsPlugin}
     */
    public static HiveConversionsPlugin conversionPlugin() {
        return CONVERSIONS_PLUGIN;
    }


    /**
     * Retrieve the {@link HivePlatform}.
     *
     * @return the {@link HivePlatform}
     */
    public static HivePlatform platform() {
        return HivePlatform.getInstance();
    }

}
