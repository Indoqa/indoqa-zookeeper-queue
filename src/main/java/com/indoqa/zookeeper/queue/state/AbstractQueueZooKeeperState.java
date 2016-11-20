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
package com.indoqa.zookeeper.queue.state;

import static org.apache.zookeeper.CreateMode.PERSISTENT_SEQUENTIAL;

import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import com.indoqa.zookeeper.AbstractZooKeeperState;
import com.indoqa.zookeeper.queue.item.ItemDescription;
import com.indoqa.zookeeper.queue.payload.PayloadConsumer;
import com.indoqa.zookeeper.queue.payload.PayloadConverter;

public abstract class AbstractQueueZooKeeperState extends AbstractZooKeeperState {

    private static final String KEY_BASE_PATH = "base-path";
    private static final String KEY_PAYLOAD_CONVERTER = "payload-converter";
    private static final String KEY_PAYLOAD_CONSUMER = "payload-consumer";

    private static final String PATH_QUEUES = "queues";
    private static final String PATH_ASSIGNED = "assigned";

    private static final int MAX_CHILDREN = 100000;

    protected AbstractQueueZooKeeperState(String name) {
        super(name);
    }

    public static void setBasePath(Map<String, Object> variables, String basePath) {
        variables.put(KEY_BASE_PATH, basePath);
    }

    public static void setPayloadConsumer(Map<String, Object> variables, PayloadConsumer<?> consumer) {
        variables.put(KEY_PAYLOAD_CONSUMER, consumer);
    }

    public static void setPayloadConverter(Map<String, Object> variables, PayloadConverter<?> converter) {
        variables.put(KEY_PAYLOAD_CONVERTER, converter);
    }

    @Override
    public void process(WatchedEvent event) {
        // default does nothing
    }

    protected final String createItem(String queueName, byte[] data) throws KeeperException {
        String pathTemplate = this.getNewItemPathTemplate(queueName);
        return this.createNode(pathTemplate, data, PERSISTENT_SEQUENTIAL);
    }

    protected final String createNewQueue() throws KeeperException {
        String queuePath = this.createNode(this.getNewQueuePathTemplate(), null, PERSISTENT_SEQUENTIAL);
        return this.getLastName(queuePath);
    }

    protected final void deleteQueue(String queueName) throws KeeperException {
        String queuePath = this.getQueuePath(queueName);
        try {
            this.deleteNode(queuePath);
        } catch (KeeperException.NoNodeException e) {
            this.logger.debug("Queue '{}' was not deleted because it did not exist.", queueName, e);
            // has been deleted already -> ignore
        }
    }

    protected final String getAssignedItemPath(ItemDescription description) {
        return combinePath(this.getAssignedPath(), description.getReference());
    }

    protected final String getAssignedPath() {
        return combinePath(this.getBasePath(), PATH_ASSIGNED);
    }

    protected final String getBasePath() {
        return this.getEnvironmentValue(KEY_BASE_PATH);
    }

    protected final String getItemPath(ItemDescription description) {
        return combinePath(this.getQueuesPath(), description.getFullPath());
    }

    protected final String getNewItemPathTemplate(String queueName) {
        return combinePath(this.getQueuesPath(), queueName, "item-");
    }

    protected final String getNewQueuePathTemplate() {
        return combinePath(this.getQueuesPath(), "queue-");
    }

    protected final PayloadConsumer<Object> getPayloadConsumer() {
        return this.getEnvironmentValue(KEY_PAYLOAD_CONSUMER);
    }

    protected final PayloadConverter<Object> getPayloadConverter() {
        return this.getEnvironmentValue(KEY_PAYLOAD_CONVERTER);
    }

    protected final String getQueuePath(String queueName) {
        return combinePath(this.getQueuesPath(), queueName);
    }

    protected final String getQueuesPath() {
        return combinePath(this.getBasePath(), PATH_QUEUES);
    }

    protected final boolean isWritableQueue(String queueName) throws KeeperException {
        if (queueName == null) {
            return false;
        }

        Stat stat = this.getStat(this.getQueuePath(queueName));
        if (stat == null) {
            return false;
        }

        return stat.getNumChildren() < MAX_CHILDREN;
    }

    protected final boolean lockItem(ItemDescription description) throws KeeperException {
        String assignedItemPath = this.getAssignedItemPath(description);

        if (this.exists(assignedItemPath)) {
            return false;
        }

        if (!this.obtainLock(assignedItemPath)) {
            return false;
        }

        if (!this.exists(this.getItemPath(description))) {
            this.unlockItem(description);
            return false;
        }

        return true;
    }

    protected final void removeItem(ItemDescription description) {
        try {
            this.deleteNode(this.getItemPath(description));
        } catch (KeeperException e) {
            this.logger.error("Could not delete queue item", e);
        }
    }

    protected final void unlockItem(ItemDescription description) throws KeeperException {
        this.deleteNode(this.getAssignedItemPath(description));
    }
}
