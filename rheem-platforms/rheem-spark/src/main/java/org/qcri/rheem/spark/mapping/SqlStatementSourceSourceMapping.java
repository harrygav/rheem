package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.SqlStatementSource;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.spark.operators.SparkSqlStatementSource;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link SqlStatementSource} to {@link SparkSqlStatementSource}.
 */
public class SqlStatementSourceSourceMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                SparkPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "source", new SqlStatementSource("", null), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SqlStatementSource>(
                (matchedOperator, epoch) -> new SparkSqlStatementSource(matchedOperator).at(epoch)
        );
    }
}
