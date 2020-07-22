package org.qcri.rheem.phoenix;


import org.qcri.rheem.phoenix.platform.PhoenixPlatform;
import org.qcri.rheem.phoenix.plugin.PhoenixConversionsPlugin;
import org.qcri.rheem.phoenix.plugin.PhoenixPlugin;

/**
 * Register for relevant components of this module.
 */
public class Phoenix {

    private final static PhoenixPlugin PLUGIN = new PhoenixPlugin();

    private final static PhoenixConversionsPlugin CONVERSIONS_PLUGIN = new PhoenixConversionsPlugin();

    /**
     * Retrieve the {@link PhoenixPlugin}.
     *
     * @return the {@link PhoenixPlugin}
     */
    public static PhoenixPlugin plugin() {
        return PLUGIN;
    }

    /**
     * Retrieve the {@link PhoenixConversionsPlugin}.
     *
     * @return the {@link PhoenixConversionsPlugin}
     */
    public static PhoenixConversionsPlugin conversionPlugin() {
        return CONVERSIONS_PLUGIN;
    }


    /**
     * Retrieve the {@link PhoenixPlatform}.
     *
     * @return the {@link PhoenixPlatform}
     */
    public static PhoenixPlatform platform() {
        return PhoenixPlatform.getInstance();
    }

}
