package org.qcri.rheem.hive.platform;

import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.jdbc.platform.JdbcPlatformTemplate;

/**
 * {@link Platform} implementation for Apache Hive.
 */
public class HivePlatform extends JdbcPlatformTemplate {

    private static final String PLATFORM_NAME = "Apache Hive";

    private static final String CONFIG_NAME = "hive";

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
    public String getJdbcDriverClassName() {
        return org.apache.hive.jdbc.HiveDriver.class.getName();
    }

}