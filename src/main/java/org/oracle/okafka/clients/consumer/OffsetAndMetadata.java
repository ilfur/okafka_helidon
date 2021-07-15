/*
** OKafka Java Client version 0.8.
**
** Copyright (c) 2019, 2020 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.oracle.okafka.clients.consumer;

import java.io.Serializable;

/**
 * This feature is not yet supported.
 */
public class OffsetAndMetadata implements Serializable {
    private final long offset;
    private final String metadata;
    public static final long INVALID_OFFSET = -1L;
    public static final String NO_METADATA = "";
    /**
     * Construct a new OffsetAndMetadata object for committing through {@link KafkaConsumer}.
     * @param offset The offset to be committed
     * @param metadata Non-null metadata
     */
    public OffsetAndMetadata(long offset, String metadata) {
        this.offset = offset;
        // The server converts null metadata to an empty string. So we store it as an empty string as well on the client
        // to be consistent.
        if (metadata == null)
            this.metadata = NO_METADATA;
        else
            this.metadata = metadata;
    }

    /**
     * Construct a new OffsetAndMetadata object for committing through {@link KafkaConsumer}. The metadata
     * associated with the commit will be empty.
     * @param offset The offset to be committed
     */
    public OffsetAndMetadata(long offset) {
        this(offset, "");
    }

    public long offset() {
        return offset;
    }

    public String metadata() {
        return metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OffsetAndMetadata that = (OffsetAndMetadata) o;

        if (offset != that.offset) return false;
        return metadata.equals(that.metadata);
    }

    @Override
    public int hashCode() {
        int result = (int) (offset ^ (offset >>> 32));
        result = 31 * result + metadata.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "OffsetAndMetadata{" +
                "offset=" + offset +
                ", metadata='" + metadata + '\'' +
                '}';
    }
}