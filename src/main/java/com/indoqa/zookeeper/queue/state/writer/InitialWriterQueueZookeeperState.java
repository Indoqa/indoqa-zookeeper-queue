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

import static com.indoqa.zookeeper.queue.state.writer.ExecutingWriterQueueZooKeeperState.EXECUTING_WRITER_STATE;

import org.apache.zookeeper.KeeperException;

import com.indoqa.zookeeper.queue.state.AbstractQueueZooKeeperState;

public final class InitialWriterQueueZookeeperState extends AbstractQueueZooKeeperState {

    public static final InitialWriterQueueZookeeperState INITIAL_WRITER_STATE = new InitialWriterQueueZookeeperState();

    private InitialWriterQueueZookeeperState() {
        super("Initialize Writer");
    }

    @Override
    protected void onStart() throws KeeperException {
        super.onStart();

        this.buildNodeStructure();
        this.transitionTo(EXECUTING_WRITER_STATE);
    }

    private void buildNodeStructure() throws KeeperException {
        this.logger.debug("Building initial node structure");

        this.ensureNodeExists("/");
        this.ensureNodeExists(this.getBasePath());
        this.ensureNodeExists(this.getQueuesPath());
    }
}
