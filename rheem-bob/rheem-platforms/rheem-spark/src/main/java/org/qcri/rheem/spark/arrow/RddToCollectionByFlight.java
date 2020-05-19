package org.qcri.rheem.spark.arrow;

import org.apache.arrow.flight.*;
import org.apache.arrow.flight.FlightClient.ClientStreamListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;

import org.qcri.rheem.core.arrow.data.ArrowTypeMapping;
import org.qcri.rheem.core.arrow.utils.FlightClientFactory;
import org.qcri.rheem.core.arrow.utils.FlightServerFactory;
import org.qcri.rheem.core.arrow.utils.FlightUtils;
import org.qcri.rheem.core.arrow.utils.VectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: Du Qinghua
 * @date: 2020/5/19-09:42
 * @file: RddToCollectionByFlight
 */
public class RddToCollectionByFlight {
    private static final Logger logger = LoggerFactory.getLogger(RddToCollectionByFlight.class);

    public static List<Object> convertRddToCollection(JavaRDD<?> inputRdd) {
        // 构建flight server
        try (FlightServerFactory serverFactory = new FlightServerFactory();
             //根据location创建server
             FlightServer server = serverFactory.creatServer(FlightUtils.getDefaultLocation())) {
            // 启动server
            server.start();
            logger.info("=======flight server startup=======");

            inputRdd.foreachPartition(iterator -> {
                // worker端，在每个分区构建flight client发送数据
                int partitionId = TaskContext.getPartitionId();
                logger.info("the partition is :" + partitionId);

                // 构建client
                try (FlightClientFactory clientFactory = new FlightClientFactory();
                     //创建连接server location的client
                     FlightClient client = clientFactory.creatClient(FlightUtils.getDefaultLocation())) {

                    BufferAllocator fieldAllocator = new RootAllocator(Long.MAX_VALUE);
                    //将简单的rdd数据转换为arrow vector
                    FieldVector partitionDataVector = VectorUtils.iteratorToVector(fieldAllocator,
                            new ArrowTypeMapping(), iterator);
                    VectorSchemaRoot partitionDataRoot = VectorSchemaRoot.of(partitionDataVector);//使用已有的vector生成VectorSchemaRoot
                    ClientStreamListener clientStreamListener = client.startPut(FlightUtils.getDefaultDescriptor(),
                            partitionDataRoot, new SyncPutListener());
                    // 发送数据
                    logger.info("=======flight client send data=======");
                    clientStreamListener.putNext();
                    clientStreamListener.completed();
                    // 等待数据上传完毕
                    clientStreamListener.getResult();
                }
            });

            // driver端，接收数据 目前仅考虑一维list的传输，因此只有一个list
            final List<Object> result = new ArrayList<>();
            try (FlightClientFactory clientFactory = new FlightClientFactory();
                 FlightClient client = clientFactory.creatClient(FlightUtils.getDefaultLocation())) {
                // 依次获取server的每个endpoint中的数据
                client.getInfo(FlightUtils.getDefaultDescriptor())
                        .getEndpoints()
                        .forEach(flightEndpoint -> {
                            try (FlightStream flightStream = client.getStream(flightEndpoint.getTicket())) {
                                flightStream.next();
                                FieldVector vector = flightStream.getRoot().getVector(0);
                                //将arrow vector转换为list
                                final List<Object> partitionList = VectorUtils.vectorToList(vector);
                                result.addAll(partitionList);
                                vector.close();
                            } catch (Exception e) {
                                e.printStackTrace();
                                logger.info("driver get data error :" + e);
                            }
                        });
            }
            return result;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
