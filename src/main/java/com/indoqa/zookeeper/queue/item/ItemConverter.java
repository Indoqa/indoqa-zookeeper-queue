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

import java.nio.ByteBuffer;

public final class ItemConverter {

    private static final int VERSION_SIZE = 2;
    private static final int ERROR_COUNT_SIZE = 2;

    private ItemConverter() {
        // hide utility class constructor
    }

    public static Item deserializeItem(byte[] data) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);

        Item result = new Item();

        result.setVersion(byteBuffer.getShort());
        result.setErrorCount(byteBuffer.getShort());

        byte[] payload = new byte[byteBuffer.remaining()];
        byteBuffer.get(payload);
        result.setPayload(payload);

        return result;
    }

    public static byte[] serialize(Item item) {
        int totalSize = VERSION_SIZE + ERROR_COUNT_SIZE + item.getPayload().length;

        ByteBuffer byteBuffer = ByteBuffer.allocate(totalSize);
        byteBuffer.putShort(item.getVersion());
        byteBuffer.putShort(item.getErrorCount());
        byteBuffer.put(item.getPayload());

        return byteBuffer.array();
    }
}
