/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.streamnative.connectors.kafka;

import org.apache.avro.Schema;

/**
 * The avro schemas used for tests.
 */
public final class AvroTestSchemas {

    public static final String USER_SCHEMA_DEF = "{\"namespace\": \"example.avro\", \"type\": \"record\", "
        + "\"name\": \"User\", \"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}";
    public static final String ACCT_SCHEMA_DEF = "{\"namespace\": \"example.avro\", \"type\": \"record\", "
        + "\"name\": \"Account\","
        + "\"fields\": [{\"name\": \"accountNumber\", \"type\": \"string\"}]}";

    public static final Schema USER_SCHEMA = new Schema.Parser().parse(USER_SCHEMA_DEF);
    public static final Schema ACCT_SCHEMA = new Schema.Parser().parse(ACCT_SCHEMA_DEF);

}
