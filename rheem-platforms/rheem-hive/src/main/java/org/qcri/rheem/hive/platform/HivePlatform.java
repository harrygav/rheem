package org.qcri.rheem.hive.platform;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.jdbc.platform.JdbcPlatformTemplate;

/**
 * {@link Platform} implementation for Apache Hive.
 */
public class HivePlatform extends JdbcPlatformTemplate {

    private static final String PLATFORM_NAME = "Apache Hive";

    private static final String CONFIG_NAME = "hive";

    private static final String DEFAULT_CONFIG_FILE = "rheem-hive-defaults.properties";

    private static HivePlatform instance = null;

    public static HivePlatform getInstance() {
        if (instance == null) {
            instance = new HivePlatform();
        }
        return instance;
    }

    protected HivePlatform() {
        super(PLATFORM_NAME, CONFIG_NAME);
    }

    @Override
    public void configureDefaults(Configuration configuration) {
        configuration.load(ReflectionUtils.loadResource(DEFAULT_CONFIG_FILE));
    }

    @Override
    public String getJdbcDriverClassName() {
        return org.apache.hive.jdbc.HiveDriver.class.getName();
    }

}