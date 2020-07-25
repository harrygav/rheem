package org.qcri.rheem.hive.operators;

import org.qcri.rheem.jdbc.operators.JdbcExecutionOperator;
import org.qcri.rheem.hive.platform.HivePlatform;

public interface HiveExecutionOperator extends JdbcExecutionOperator {

    @Override
    default HivePlatform getPlatform() {
        return HivePlatform.getInstance();
    }

}