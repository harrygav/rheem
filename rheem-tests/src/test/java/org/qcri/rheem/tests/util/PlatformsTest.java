package org.qcri.rheem.tests.util;

import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.spark.platform.SparkPlatform;

/**
 * {@link PlatformsTest} it an enum that contains the platforms that
 * can be asigned to an operator
 */
public enum PlatformsTest {

    //Java Platform instance
    Java(JavaPlatform.getInstance(), "Java"),

    //Spark Platform instance
    Spark(SparkPlatform.getInstance(), "Spark");

    /**
     * Instance of the platform that is on the enum {@link PlatformsTest}
     */
    private final Platform platform;

    /**
     * name of the platform, this will be use normally on the case of
     * debugging
     */
    private final String name;

    /**
     * retrive the platform form the instance of the enum {@link PlatformsTest}
     * @return a {@link Platform} of the Rheem have implemented
     */
    public Platform getPlatform() {
        return platform;
    }

    /**
     * retrieve the name of the platform
     * @return a {@link String} which have as content the name of the platform
     */
    public String getName() {
        return name;
    }

    /**
     * Constructor of the {@link PlatformsTest}
     *
     * @param platform is an instance of the {@link Platform} that Rheem have implemented
     * @param name is the name that will use to identify the instance of {@link PlatformsTest}
     */
    private PlatformsTest(Platform platform, String name) {
        this.platform = platform;
        this.name = name;
    }


}
