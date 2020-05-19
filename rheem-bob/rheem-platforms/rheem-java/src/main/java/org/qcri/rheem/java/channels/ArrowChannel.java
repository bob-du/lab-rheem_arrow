package org.qcri.rheem.java.channels;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.qcri.rheem.core.platform.ChannelInstance;

import java.util.stream.Stream;

/**
 * @author: Du Qinghua
 * @file: ArrowChannel
 */
public interface ArrowChannel extends ChannelInstance {
    /**
     * Provide the producer's result to a consumer.
     *
     * @return the producer's result
     */
    VectorSchemaRoot provideVectorSchemaRoot();
}
