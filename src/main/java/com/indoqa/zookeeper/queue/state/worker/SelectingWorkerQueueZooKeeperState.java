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

import static com.indoqa.zookeeper.queue.state.worker.WaitingWorkerState.WAITING_WORKER_STATE;

import java.util.LinkedList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import com.indoqa.zookeeper.queue.item.ItemDescription;
import com.indoqa.zookeeper.queue.state.AbstractQueueZooKeeperState;

public final class SelectingWorkerQueueZooKeeperState extends AbstractQueueZooKeeperState {

    public static final SelectingWorkerQueueZooKeeperState SELECTING_WORKER_STATE = new SelectingWorkerQueueZooKeeperState();

    private static final int MAX_LOCAL_QUEUE_SIZE = 1000;

    private final List<ItemDescription> localQueue = new LinkedList<>();

    private SelectingWorkerQueueZooKeeperState() {
        super("Selecting Item");
    }

    @Override
    protected void onStart() throws KeeperException {
        super.onStart();

        if (this.localQueue.isEmpty()) {
            this.findTasks();
        }

        while (!this.localQueue.isEmpty()) {
            ItemDescription description = this.localQueue.remove(0);

            if (this.lockItem(description)) {
                this.transitionTo(new ProcessingWorkerQueueZooKeeperState(description));
                return;
            }
        }

        // we could not acquire a lock for a task -> return to "Waiting"
        this.transitionTo(WAITING_WORKER_STATE);
    }

    private void cleanPendingLocks() throws KeeperException {
        this.logger.debug("Cleaning pending locks.");

        List<String> assigned = this.getChildren(this.getAssignedPath());
        for (String eachAssigned : assigned) {
            String lockPath = combinePath(this.getAssignedPath(), eachAssigned);
            Stat stat = this.getStat(lockPath);
            if (stat == null) {
                continue;
            }

            if (stat.getEphemeralOwner() == this.zooKeeper.getSessionId()) {
                this.logger.warn("Found pending lock from own session {}. Deleting it now.", this.zooKeeper.getSessionId());
                this.deleteNode(lockPath);
            }
        }
    }

    private void findTasks() throws KeeperException {
        try {
            this.cleanPendingLocks();
        } catch (Exception e) {
            this.logger.error("Cleaning pending locks failed.", e);
        }

        List<String> queueNames = this.getSortedChildren(this.getQueuesPath());
        if (queueNames.isEmpty()) {
            return;
        }

        String writeQueueName = queueNames.get(queueNames.size() - 1);

        for (String eachQueueName : queueNames) {
            List<String> items = this.getSortedChildren(this.getQueuePath(eachQueueName));

            if (items.isEmpty() && !eachQueueName.equals(writeQueueName)) {
                this.logger.info("Deleting expired sub-queue {}", eachQueueName);
                this.deleteQueue(eachQueueName);
                continue;
            }

            int transferCount = Math.min(items.size(), MAX_LOCAL_QUEUE_SIZE - this.localQueue.size());
            items.stream().limit(transferCount).forEach(taskName -> this.localQueue.add(new ItemDescription(taskName, eachQueueName)));

            if (this.localQueue.size() >= MAX_LOCAL_QUEUE_SIZE) {
                break;
            }
        }

        this.logger.debug("Moved {} items into the local queue.", this.localQueue.size());
    }
}
