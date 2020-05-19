package org.qcri.rheem.core.arrow.utils;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * @author: Du Qinghua
 * @date: 2020/5/19-10:06
 * @file: FlightClientFactory
 */
public class FlightClientFactory implements AutoCloseable {

    private final BufferAllocator allocator = new RootAllocator();

    /**
     * 创建默认的server
     *
     * @return flight server
     */
    public FlightClient creatClient(Location location) {
        return FlightClient.builder()
                .allocator(allocator)
                .location(location)
                .build();
    }

    @Override
    public void close() {
        allocator.close();
    }
}
