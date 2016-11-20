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
package com.indoqa.zookeeper.queue.state.worker;

import static com.indoqa.zookeeper.queue.state.worker.SelectingWorkerQueueZooKeeperState.SELECTING_WORKER_STATE;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.indoqa.zookeeper.queue.state.AbstractQueueZooKeeperState;

public final class WaitingWorkerState extends AbstractQueueZooKeeperState {

    public static final WaitingWorkerState WAITING_WORKER_STATE = new WaitingWorkerState();

    private WaitingWorkerState() {
        super("Waiting for Item");
    }

    @Override
    public void onStart() throws KeeperException {
        super.onStart();

        List<String> queues = this.getSortedChildrenAndWatch(this.getQueuesPath());
        for (String eachQueue : queues) {
            String queuePath = this.getQueuePath(eachQueue);

            List<String> tasks = this.getChildrenAndWatch(queuePath);
            if (!tasks.isEmpty()) {
                this.transitionTo(SELECTING_WORKER_STATE);
                return;
            }
        }

        this.logger.info("Waiting for new items.");
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == EventType.NodeChildrenChanged) {
            this.transitionTo(SELECTING_WORKER_STATE);
            return;
        }

        super.process(event);
    }
}
