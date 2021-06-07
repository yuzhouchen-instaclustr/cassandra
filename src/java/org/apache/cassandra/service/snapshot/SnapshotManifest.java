/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service.snapshot;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.config.Duration;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.databind.DeserializationFeature;

// Only serialize fields
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY,
                getterVisibility = JsonAutoDetect.Visibility.NONE,
                setterVisibility = JsonAutoDetect.Visibility.NONE)
public class SnapshotManifest
{
    static final ObjectMapper mapper = new ObjectMapper();
    static {
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @JsonProperty("files")
    public final List<String> files;

    @JsonSerialize(using = ToStringSerializer.class)
    @JsonProperty("created_at")
    public final Instant createdAt;

    @JsonSerialize(using = ToStringSerializer.class)
    @JsonProperty("expires_at")
    public final Instant expiresAt;

    private SnapshotManifest() {
        super();
        this.files = null;
        this.createdAt = null;
        this.expiresAt = null;
    }

    public SnapshotManifest(List<String> files, Duration ttl)
    {
        this.files = files;
        this.createdAt = Instant.now();
        this.expiresAt = ttl == null ? null : createdAt.plusMillis(ttl.toMilliseconds());
    }

    public List<String> getFiles()
    {
        return files;
    }

    public Instant getCreatedAt()
    {
        return createdAt;
    }

    public Instant getExpiresAt()
    {
        return expiresAt;
    }

    public void serializeToJsonFile(File outputFile) throws IOException
    {
        mapper.writeValue(outputFile, this);
    }

    public static SnapshotManifest deserializeFromJsonFile(File file) throws IOException
    {
        return mapper.readValue(file, SnapshotManifest.class);
    }
}
