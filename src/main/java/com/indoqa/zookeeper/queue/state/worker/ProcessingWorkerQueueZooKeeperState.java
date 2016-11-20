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

import org.apache.zookeeper.KeeperException;

import com.indoqa.zookeeper.queue.item.Item;
import com.indoqa.zookeeper.queue.item.ItemConverter;
import com.indoqa.zookeeper.queue.item.ItemDescription;
import com.indoqa.zookeeper.queue.state.AbstractQueueZooKeeperState;

public final class ProcessingWorkerQueueZooKeeperState extends AbstractQueueZooKeeperState {

    private final ItemDescription description;

    public ProcessingWorkerQueueZooKeeperState(ItemDescription description) {
        super("Processing Item");

        this.description = description;
    }

    @Override
    protected void onStart() throws KeeperException {
        super.onStart();

        try {
            String itemPath = this.getItemPath(this.description);

            byte[] data = this.getData(itemPath);
            Item item = ItemConverter.deserializeItem(data);

            this.processItem(item);

            this.removeItem(this.description);
            this.transitionTo(SELECTING_WORKER_STATE);
        } finally {
            this.unlockItem(this.description);
        }
    }

    private void processItem(Item item) {
        Object payload = this.getPayloadConverter().deserialize(item.getPayload());
        this.getPayloadConsumer().consume(payload);
    }
}
