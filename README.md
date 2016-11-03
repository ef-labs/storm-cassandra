Storm Cassandra Integration
===========================

Integrates Storm and Cassandra by providing generic and configurable components that read  
and write from Cassandra. 

How the Storm `Tuple` data is written to Cassandra is dynamically configurable -- you
provide classes that "determine" a table, row key, and column name/values, and the 
components will write the iteraction with Cassandra.

### Project Location
Primary development of storm-cassandra will take place at: 
https://github.com/hmsonline/storm-cassandra

Point/stable (non-SNAPSHOT) release souce code will be pushed to:
https://github.com/nathanmarz/storm-contrib

Maven artifacts for releases will be available on maven central.

### Building from Source

		$ mvn install

### Cassandra Configuration

All storm and trident components expect cassandra connection information to be included in the storm topology configuration. 
 The configuration is normally parsed fro the storm configuration using the ```cassandra-config```. Typically, this should
 contain at least the seed hosts and port. See the JsonClusterConfigurator for other settings.
 
```
{
    "cassandra-config": {
        "seeds": [ "cassandra1", "cassandra2" ],
        "port": 9042
    }
}
```

You can also create the configuration in code:
```
    Map<String, Object> cassandraConfig = new HashMap<>();
    cassandraConfig.put(CONFIG_SEEDS, new String[] { "cassandra1", "cassandra2" });
    cassandraConfig.put(CONFIG_PORT, 9042);
    Map<String, Object> stormConfig = new HashMap<>();
    stormConfig.put(CASSANDRA_CONFIG_KEY, clusterConfig);
```

At runtime, the components will use the key name ("cassandra-config" by default) to cache Cassandra sessions for the whole worker
 process (i.e. there will be one session per topology and worker node).

If you need to work with multiple cassandra clusters, you can override the config key on the components.

### Concrete Component Overview

Storm:

* CassandraBolt; writes incoming tuples to Cassandra, using a UpsertMapper to map tuples to cassandra fields.
* CassandraBatchingBolt; writes incoming tuples to Cassandra in batches on a separate thread loop.
  Note that there is no performance benefit to using this over the CassandraBolt - they both use a similar parallelism
  approach, except that the batching bolt additionally supports a max batch size that can be used to control batch sizes.
  This component is less efficient than the CassandraBolt because it waits for each batch to be fully processed before 
  continuing.
* TransactionalCassandraBatchingBolt; collects incoming tuples and writes them when the transaction is finished.
* CassandraLookupBolt; performs a query based on incoming tuple values and emits new columns into the stream. If multiple records
  are returned from Cassandra, one tuple is emitted for each record.

Trident:

* CassandraBackingMap; a backing map implementation for Cassandra.
* CassandraMapStateFactory; a state factory that provides a MapState with a CassandraBackingMap, with optional caching.
* CassandraLookupFunction; performs a query based on incoming tuple values and emits new columns into the stream. If multiple records
  are returned from Cassandra, one tuple is emitted for each record. 

### Parallelism

Most components perform cassandra operations in asynchronously in parallel. 

* The storm components (CassandraBolt, TransactionalCassandraBatchingBolt, CassandraLookupBolt) will all start their
  cassandra operations and exit the execute() method. The ack (if applicable) is done once the operation is completed on
  the shared executor thread.
* The CassandraBatchingBolt and CassandraBackingMap will execute all statements asynchronously, in parallel, and wait for 
  them to finish. While they wait for processing to finish before starting the next batch there is no limit on the number of 
  parallel operations within the batch.

Given this setup, there is a risk of flooding Cassandra with a lot of traffic. There are two mechanisms that can be used to
mitigate this.

* Edit the value of topology.max.spout.pending in the storm.yaml configuration file to limit the number of unacked tuples in 
  the topology, and use ACK_ON_WRITE (for the storm components). 
  The default is no limit. Hortonworks recommends that topologies using the core-storm API start with a value of 1000 and 
  slowly decrease the value as necessary. Toplogies using the Trident API should start with a much lower value, between 1 and 5.
* For controlling trident topologies more detailed than the number of in-flight batches, the CassandraStateFactory implements
  throttling with the setMaxParallelism() method.

In case Cassandra does get overloaded, the datastax driver throws an OverloadedException, which handled as a retryable error. 

# Examples

The "examples" directory contains two examples:

* CassandraReachTopology

* PersistentWordCount

## Cassandra Reach Topology

The [`CassandraReachTopology`](https://github.com/hmsonline/storm-cassandra/blob/master/examples/src/main/java/com/hmsonline/storm/cassandra/example/CassandraReachTopology.java) 
example is a Storm [Distributed RPC](https://github.com/nathanmarz/storm/wiki/Distributed-RPC) example 
that is essentially a clone of Nathan Marz' [`ReachTopology`](https://github.com/nathanmarz/storm-starter/blob/master/src/jvm/storm/starter/ReachTopology.java), 
that instead of using in-memory data stores is backed by a [Cassandra](http://cassandra.apache.org/) database and uses generic 
*storm-cassandra* trident lookup functions to query the database.

## Persistent Word Count  
The sample [`PersistentWordCount`](https://github.com/hmsonline/storm-cassandra/blob/master/examples/src/main/java/com/hmsonline/storm/cassandra/example/PersistentWordCount.java) 
topology illustrates the basic usage of the Cassandra Bolt implementation. It reuses the [`TestWordSpout`](https://github.com/nathanmarz/storm/blob/master/src/jvm/backtype/storm/testing/TestWordSpout.java) 
spout and [`TestWordCounter`](https://github.com/nathanmarz/storm/blob/master/src/jvm/backtype/storm/testing/TestWordCounter.java) 
bolt from the Storm tutorial, and adds an instance of `AbstractCassandraBolt` to persist the results.

## Running the Samples


## Preparation
In order to run the examples, you will need a Cassandra database running on `localhost:9160`.

### Build the Example Source

		$ cd examples
		$ mvn install
	
### Create Cassandra Schema and Sample Data
Install and run [Apache Cassandra](http://cassandra.apache.org/).

Create the sample schema using `cassandra-cli`:

		$ cd schema
		$ cat cassandra_schema.txt | cassandra-cli -h localhost

## Running the Cassandra Reach Topology

To run the `CassandraReachTopology` execute the following maven command:

		$ mvn exec:java -Dexec.mainClass=com.hmsonline.storm.cassandra.example.CassandraReachTopology

Among the output, you should see the following:

	Reach of http://github.com/hmsonline: 3
	Reach of http://github.com/nathanmarz: 3
	Reach of http://github.com/ptgoetz: 4
	Reach of http://github.com/boneill: 0

To enable logging of all tuples sent within the topology, run the following command:

		$ mvn exec:java -Dexec.mainClass=com.hmsonline.storm.cassandra.example.CassandraReachTopology -Ddebug=true

## Running the Persistent Word Count Example

The `PersistentWordCount` example build the following topology:

	TestWordSpout ==> TestWordCounter ==> CassandraBolt
	
**Data Flow**

1. `TestWordSpout` emits words at random from a pre-defined list.
2. `TestWordCounter` receives a word, updates a counter for that word,
and emits a tuple containing the word and corresponding count ("word", "count").
3. The `AbstractCassandraBolt` receives the ("word", "count") tuple and writes it to the
Cassandra database using the word as the row key.


Run the `PersistentWordCount` topology:

		$ mvn exec:java -Dexec.mainClass=com.hmsonline.storm.cassandra.example.PersistentWordCount
	
View the end result in `cassandra-cli`:

		$ cassandra-cli -h localhost
		[default@unknown] use stormks;
		[default@stromks] list stormcf;
	
The output should resemble the following:

		Using default limit of 100
		-------------------
		RowKey: nathan
		=> (column=count, value=22, timestamp=1322332601951001)
		=> (column=word, value=nathan, timestamp=1322332601951000)
		-------------------
		RowKey: mike
		=> (column=count, value=11, timestamp=1322332600330001)
		=> (column=word, value=mike, timestamp=1322332600330000)
		-------------------
		RowKey: jackson
		=> (column=count, value=17, timestamp=1322332600633001)
		=> (column=word, value=jackson, timestamp=1322332600633000)
		-------------------
		RowKey: golda
		=> (column=count, value=31, timestamp=1322332602155001)
		=> (column=word, value=golda, timestamp=1322332602155000)
		-------------------
		RowKey: bertels
		=> (column=count, value=16, timestamp=1322332602257000)
		=> (column=word, value=bertels, timestamp=1322332602255000)
		
		5 Rows Returned.
		Elapsed time: 8 msec(s).


## License

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
