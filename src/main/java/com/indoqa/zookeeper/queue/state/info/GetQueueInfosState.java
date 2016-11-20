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
package com.indoqa.zookeeper.queue.state.info;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import com.indoqa.zookeeper.Execution;
import com.indoqa.zookeeper.queue.state.AbstractQueueZooKeeperState;

public final class GetQueueInfosState extends AbstractQueueZooKeeperState {

    public static final GetQueueInfosState GET_QUEUE_INFOS_STATE = new GetQueueInfosState();

    private static final String QUEUE_INFOS_KEY = "queue-infos";

    private GetQueueInfosState() {
        super("Get QueueInfos");
    }

    public static List<QueueInfo> getQueueInfos(Execution execution) {
        return execution.getEnvironmentValue(QUEUE_INFOS_KEY);
    }

    private static void setQueueInfos(Execution execution, List<QueueInfo> queueInfos) {
        execution.setEnvironmentValue(QUEUE_INFOS_KEY, queueInfos);
    }

    @Override
    protected void onStart() throws KeeperException {
        // terminate the execution when this state finishes
        this.terminate();

        List<QueueInfo> queueInfos = new ArrayList<>();
        setQueueInfos(this.execution, queueInfos);

        List<String> queueNames = this.getSortedChildren(this.getQueuesPath());
        if (queueNames.isEmpty()) {
            return;
        }

        for (String eachQueueName : queueNames) {
            Stat stat = this.getStat(this.getQueuePath(eachQueueName));
            queueInfos.add(QueueInfo.create(eachQueueName, stat.getNumChildren()));
        }
    }
}
