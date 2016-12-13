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
package com.indoqa.zookeeper.queue.state.writer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.zookeeper.KeeperException;

import com.indoqa.zookeeper.queue.item.Item;
import com.indoqa.zookeeper.queue.item.ItemConverter;
import com.indoqa.zookeeper.queue.state.AbstractQueueZooKeeperState;

public final class ExecutingWriterQueueZooKeeperState extends AbstractQueueZooKeeperState {

    public static final ExecutingWriterQueueZooKeeperState EXECUTING_WRITER_STATE = new ExecutingWriterQueueZooKeeperState();

    private static final int MAX_POLL_WAIT = 10000;
    private static final int MAX_OFFER_WAIT = 10000;
    private static final int MAX_LOCAL_QUEUE_SIZE = 1000;
    private static final int COUNT_DOWN_START = 1000;

    private final BlockingQueue<Item> pendingItems = new LinkedBlockingQueue<>(MAX_LOCAL_QUEUE_SIZE);

    private String lastWriteQueueName;
    private int refreshCountDown;

    private ExecutingWriterQueueZooKeeperState() {
        super("Writing Items");
    }

    public void addPayload(Object payload) {
        this.logger.debug("Inserting payload into local queue.");

        Item item = this.createItem(payload);
        try {
            if (this.pendingItems.offer(item, MAX_OFFER_WAIT, MILLISECONDS)) {
                return;
            }
        } catch (@SuppressWarnings("unused") InterruptedException e) {
            // ignore
        }

        throw new IllegalStateException("Could not insert payload into local queue, because there was no space available.");
    }

    @Override
    protected void onStart() throws KeeperException {
        super.onStart();

        try {
            Item item = this.pendingItems.poll(MAX_POLL_WAIT, MILLISECONDS);
            if (item != null) {
                String queueName = this.getWriteQueueName();
                byte[] data = ItemConverter.serialize(item);
                this.createItem(queueName, data);
            }
        } catch (@SuppressWarnings("unused") InterruptedException e) {
            // ignore
        } catch (KeeperException e) {
            // force a refresh of the write queue before the next write attempt
            this.refreshCountDown = 0;
            throw e;
        }

        this.transitionTo(this);
    }

    private Item createItem(Object payload) {
        Item result = new Item();

        result.setErrorCount((short) 0);
        result.setVersion(Item.VERSION);
        result.setPayload(this.getPayloadConverter().serialize(payload));

        return result;
    }

    private String getWriteQueueName() throws KeeperException {
        if (--this.refreshCountDown > 0) {
            return this.lastWriteQueueName;
        }

        this.lastWriteQueueName = null;
        this.refreshCountDown = COUNT_DOWN_START;

        List<String> queueNames = this.getSortedChildren(this.getQueuesPath());
        if (!queueNames.isEmpty()) {
            String lastQueueName = queueNames.get(queueNames.size() - 1);
            if (this.isWritableQueue(lastQueueName)) {
                this.lastWriteQueueName = lastQueueName;
                return this.lastWriteQueueName;
            }
        }

        this.lastWriteQueueName = this.createNewQueue();
        return this.lastWriteQueueName;
    }
}
