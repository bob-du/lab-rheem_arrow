package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.spark.arrow.RddToCollectionByFlight;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;

import java.util.Collection;
import java.util.List;


/**
 * @author: Du Qinghua
 * @file: SparkStreamOperator
 */
public  class SparkStreamOperator<Type> extends UnaryToUnaryOperator<Type,Type>
            implements  SparkExecutionOperator{

    public SparkStreamOperator(DataSetType<Type> type) {
        super(type, type, false);

    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(ChannelInstance[] inputs,
                                                   ChannelInstance[] outputs, SparkExecutor sparkExecutor,
                                                   OptimizationContext.OperatorContext operatorContext) {
        RddChannel.Instance input= (RddChannel.Instance) inputs[0];
        StreamChannel.Instance output=(StreamChannel.Instance) outputs[0];

        JavaRDD<Type> javaRDD = input.provideRdd();
        //把主要实现步骤分离出来方便测试
        Collection<?> result = RddToCollectionByFlight.convertRddToCollection(javaRDD);
        output.accept(result);

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return null;
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return null;
    }
}
