/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hmsonline.storm.cassandra.mapper;

import com.datastax.driver.core.Row;
import com.hmsonline.storm.cassandra.util.DataTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

import java.io.Serializable;
import java.util.List;

public interface LookupMapper extends Serializable {

    /**
     * Given a <code>org.apache.storm.tuple.Tuple</code> object, map the keyspace to write to.
     *
     * @param tuple The tuple to map from
     * @return The keyspace to use
     */
    String mapToKeyspace(ITuple tuple);

    /**
     * Given a <code>org.apache.storm.tuple.ITuple</code> object, map the cassandra table
     * to write to.
     *
     * @param tuple The tuple to map from
     * @return The table name to use
     */
    String mapToTable(ITuple tuple);

    /**
     * Given a <code>org.apache.storm.tuple.Tuple</code> generate a Cassandra row
     * key.
     *
     * @param tuple The tuple to map from
     * @return The row key values to use (for loading)
     */
    DataTuple mapToRowKey(ITuple tuple);

    /**
     * Declares the fields produced by the bolt using this mapper.
     */
    Fields getKeys();

    /**
     * Declares the fields produced by the bolt using this mapper.
     */
    Fields getColumns();

    /**
     * Declares the fields produced by the bolt using this mapper.
     */
    Fields getOutputFields();

    /**
     * Given a set of columns, maps to values to emit.
     */
    Values mapToValues(Row row);

}

