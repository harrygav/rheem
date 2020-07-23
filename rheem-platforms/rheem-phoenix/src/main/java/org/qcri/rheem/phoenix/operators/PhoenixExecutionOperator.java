package org.qcri.rheem.phoenix.operators;

import org.qcri.rheem.jdbc.operators.JdbcExecutionOperator;
import org.qcri.rheem.phoenix.platform.PhoenixPlatform;

public interface PhoenixExecutionOperator extends JdbcExecutionOperator {

    @Override
    default PhoenixPlatform getPlatform() {
        return PhoenixPlatform.getInstance();
    }

}