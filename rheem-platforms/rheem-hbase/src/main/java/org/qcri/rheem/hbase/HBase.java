package org.qcri.rheem.hbase;


import org.qcri.rheem.hbase.platform.HBasePlatform;
import org.qcri.rheem.hbase.plugin.HBaseConversionsPlugin;
import org.qcri.rheem.hbase.plugin.HBasePlugin;

/**
 * Register for relevant components of this module.
 */
public class HBase {

    private final static HBasePlugin PLUGIN = new HBasePlugin();

    private final static HBaseConversionsPlugin CONVERSIONS_PLUGIN = new HBaseConversionsPlugin();

    /**
     * Retrieve the {@link HBasePlugin}.
     *
     * @return the {@link HBasePlugin}
     */
    public static HBasePlugin plugin() {
        return PLUGIN;
    }

    /**
     * Retrieve the {@link HBaseConversionsPlugin}.
     *
     * @return the {@link HBaseConversionsPlugin}
     */
    public static HBaseConversionsPlugin conversionPlugin() {
        return CONVERSIONS_PLUGIN;
    }


    /**
     * Retrieve the {@link HBasePlatform}.
     *
     * @return the {@link HBasePlatform}
     */
    public static HBasePlatform platform() {
        return HBasePlatform.getInstance();
    }

}
