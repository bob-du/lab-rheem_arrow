package org.qcri.rheem.java.channels;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.AbstractChannelInstance;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Executor;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Stream;

/**
 * @author: Du Qinghua
 * @file: ArrowChannel
 */
public class VectorSchemaRootChannel extends Channel{

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(
            VectorSchemaRootChannel.class, true, false
    );

    public static final ChannelDescriptor DESCRIPTOR_MANY = new ChannelDescriptor(
            VectorSchemaRootChannel.class, true, false
    );

    public VectorSchemaRootChannel(ChannelDescriptor descriptor, OutputSlot<?> outputSlot) {
        super(descriptor, outputSlot);
        assert descriptor == DESCRIPTOR || descriptor == DESCRIPTOR_MANY;
        //
        this.markForInstrumentation();
    }

    private VectorSchemaRootChannel(VectorSchemaRootChannel parent) {
        super(parent);
    }

    @Override
    public Channel copy() {
        return new VectorSchemaRootChannel(this);
    }

    @Override
    public Instance createInstance(Executor executor,
                                                     OptimizationContext.OperatorContext producerOperatorContext,
                                                     int producerOutputIndex) {
        return new Instance(executor, producerOperatorContext, producerOutputIndex);
    }



    /**
     * {@link ChannelInstance} implementation for {@link VectorSchemaRoot}s.
     */
    public class Instance extends AbstractChannelInstance {

        private VectorSchemaRoot vectorSchemaRoot;

        private long size;

        public Instance(Executor executor,
                        OptimizationContext.OperatorContext producerOperatorContext,
                        int producerOutputIndex) {
            super(executor, producerOperatorContext, producerOutputIndex);
        }
        public VectorSchemaRoot provideVectorSchemaRoot() {
            return (VectorSchemaRoot) this.vectorSchemaRoot;
        }
        public void accept(VectorSchemaRoot vectorSchemaRoot) {
            this.vectorSchemaRoot = vectorSchemaRoot;
        }
        //传入的若为单个valuevector
        public void accept(FieldVector vector) {
            // 使用schema以及vector构建一个vector schema
            List<Field> fields = Arrays.asList(vector.getField()); // 存储vector的schema信息(vector中数据类型)
            List<FieldVector> vectors = Arrays.asList(vector);    //
            this.vectorSchemaRoot = new VectorSchemaRoot(fields, vectors);
        }



        @Override
        protected void doDispose() {
            this.vectorSchemaRoot = null;
        }

        @Override
        public OptionalLong getMeasuredCardinality() {
            return this.size == 0 ? super.getMeasuredCardinality() : OptionalLong.of(this.size);
        }

        @Override
        public VectorSchemaRootChannel getChannel() {
            return VectorSchemaRootChannel.this;
        }

    }
}
