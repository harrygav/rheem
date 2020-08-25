package org.qcri.rheem.hive.mapping;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.OperatorPattern;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.mapping.ReplacementSubplanFactory;
import org.qcri.rheem.core.mapping.SubplanPattern;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.hive.operators.HiveJoinOperator;
import org.qcri.rheem.hive.platform.HivePlatform;

import java.util.Collection;
import java.util.Collections;

public class JoinMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                HivePlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern<>(
                "join",
                new JoinOperator<>(null, null,
                        DataSetType.createDefault(Record.class), DataSetType.createDefault(Record.class)),
                false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<JoinOperator<Object, Object, Object>>(
                (matchedOperator, epoch) -> new HiveJoinOperator<>(matchedOperator).at(epoch)
        );
    }
}
