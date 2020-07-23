package org.qcri.rheem.phoenix.platform;

import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.jdbc.platform.JdbcPlatformTemplate;

/**
 * {@link Platform} implementation for Phoenix.
 */
public class PhoenixPlatform extends JdbcPlatformTemplate {

    private static final String PLATFORM_NAME = "Phoenix";

    private static final String CONFIG_NAME = "phoenix";

    private static PhoenixPlatform instance = null;

    public static PhoenixPlatform getInstance() {
        if (instance == null) {
            instance = new PhoenixPlatform();
        }
        return instance;
    }

    protected PhoenixPlatform() {
        super(PLATFORM_NAME, CONFIG_NAME);
    }

    @Override
    public String getJdbcDriverClassName() {
        return org.apache.phoenix.queryserver.client.Driver.class.getName();
    }

}
