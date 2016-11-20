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
package com.indoqa.zookeeper.queue.item;

public class ItemDescription {

    private final String name;
    private final String queueName;

    private final String fullPath;
    private final String reference;

    public ItemDescription(String name, String queueName) {
        super();

        this.name = name;
        this.queueName = queueName;

        this.fullPath = queueName + "/" + name;
        this.reference = queueName + "|" + name;
    }

    public String getFullPath() {
        return this.fullPath;
    }

    public String getName() {
        return this.name;
    }

    public String getQueueName() {
        return this.queueName;
    }

    public String getReference() {
        return this.reference;
    }

}
