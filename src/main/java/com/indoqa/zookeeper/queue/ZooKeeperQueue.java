/*
 * Licensed to the Indoqa Software Design und Beratung GmbH (Indoqa) under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Indoqa licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.indoqa.zookeeper.queue;

import static com.indoqa.zookeeper.queue.state.info.GetQueueInfosState.GET_QUEUE_INFOS_STATE;
import static com.indoqa.zookeeper.queue.state.worker.InitialWorkerQueueZooKeeperState.INITIAL_WORKER_STATE;
import static com.indoqa.zookeeper.queue.state.writer.ExecutingWriterQueueZooKeeperState.EXECUTING_WRITER_STATE;
import static com.indoqa.zookeeper.queue.state.writer.InitialWriterQueueZookeeperState.INITIAL_WRITER_STATE;

import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.indoqa.zookeeper.Execution;
import com.indoqa.zookeeper.StateExecutor;
import com.indoqa.zookeeper.queue.payload.PayloadConsumer;
import com.indoqa.zookeeper.queue.payload.PayloadConverter;
import com.indoqa.zookeeper.queue.state.AbstractQueueZooKeeperState;
import com.indoqa.zookeeper.queue.state.info.GetQueueInfosState;
import com.indoqa.zookeeper.queue.state.info.QueueInfo;

public class ZooKeeperQueue<T> implements Closeable {

    private final StateExecutor stateExecutor;

    private Execution workerExecution;
    private Execution writerExecution;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private String basePath;

    public ZooKeeperQueue(StateExecutor stateExecutor, PayloadConverter<? extends T> payloadConverter,
            PayloadConsumer<? extends T> payloadConsumer, String basePath) {
        super();

        this.stateExecutor = stateExecutor;
        this.basePath = basePath;

        Map<String, Object> enviromentValues = new HashMap<>();
        AbstractQueueZooKeeperState.setBasePath(enviromentValues, basePath);
        AbstractQueueZooKeeperState.setPayloadConverter(enviromentValues, payloadConverter);
        AbstractQueueZooKeeperState.setPayloadConsumer(enviromentValues, payloadConsumer);

        // start the state for processing items
        this.workerExecution = this.stateExecutor.executeState(INITIAL_WORKER_STATE, enviromentValues);

        // start the state for writing items
        this.writerExecution = this.stateExecutor.executeState(INITIAL_WRITER_STATE, enviromentValues);
    }

    public void addToQueue(T payload) {
        EXECUTING_WRITER_STATE.addPayload(payload);
    }

    @Override
    public void close() {
        this.logger.info("Terminating writer execution.");
        this.writerExecution.terminate();

        this.logger.info("Terminating worker execution.");
        this.workerExecution.terminate();
    }

    public List<QueueInfo> getQueueInfos() {
        Map<String, Object> enviromentValues = new HashMap<>();
        AbstractQueueZooKeeperState.setBasePath(enviromentValues, this.basePath);

        Execution execution = this.stateExecutor.executeState(GET_QUEUE_INFOS_STATE, enviromentValues);
        this.stateExecutor.waitForTermination(execution);

        return GetQueueInfosState.getQueueInfos(execution);
    }
}
