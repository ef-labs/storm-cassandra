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
package com.hmsonline.storm.cassandra.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Abstract <code>IRichBolt</code> implementation that supports batching incoming
 * <code>org.apache.storm.tuple.Tuple</code>s and processes them on a separate
 * thread.
 * <p>
 * Subclasses must implement the <code>executeBatch(List&lt;Tuple&gt; inputs)</code> method.
 * <p>
 * Subclasses may override the <code>execute(Tuple input)</code> method to implement
 * ack-on-receive.
 * <p>
 * Subclasses that override the <code>prepare()</code> and <code>cleanup()</code>
 * methods <b><i>must</i></b> call the corresponding methods on the superclass
 * (i.e. <code>super.prepare()</code> and <code>super.cleanup()</code>) to
 * ensure proper initialization and termination.
 *
 */
@SuppressWarnings("serial")
public abstract class AbstractCassandraBatchingBolt<T extends AbstractCassandraBatchingBolt> extends AbstractCassandraBolt<T> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractCassandraBatchingBolt.class);

    public static final String CASSANDRA_BATCH_MAX_SIZE = "cassandra.batch.max_size";

    protected LinkedBlockingQueue<Tuple> queue;
    private BatchThread batchThread;

    public AbstractCassandraBatchingBolt() {
        super();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        @SuppressWarnings("unchecked" )
        Map<String, Object> boltConf = (Map<String, Object>) stormConf.get(getCassandraConfigKey());
        @SuppressWarnings("unchecked" )
        int batchMaxSize = Utils.getInt(Utils.get(boltConf, CASSANDRA_BATCH_MAX_SIZE, 0));
        this.queue = new LinkedBlockingQueue<>();
        this.batchThread = new BatchThread(batchMaxSize);
        this.batchThread.start();
    }

    /**
     * Enqueues an item for processing.
     * @param input the input tuple
     */
    protected void enqueue(Tuple input) {
        this.queue.offer(input);
    }

    @Override
    public void cleanup() {
        this.batchThread.stopRunning();
        super.cleanup();
    }

    @Override
    public void execute(Tuple input) {
        this.enqueue(input);
    }

    /**
     * Process a <code>java.util.List</code> of
     * <code>backtype.storm.tuple.Tuple</code> objects that have been
     * cached/batched.
     * <p>
     * This method is analagous to the <code>execute(Tuple input)</code> method
     * defined in the bolt interface. Subclasses are responsible for processing
     * and/or ack'ing tuples as necessary. The only difference is that tuples
     * are passed in as a list, as opposed to one at a time.
     * <p>
     *
     * @param inputs The input tuples in the batch
     */
    public abstract void executeBatch(List<Tuple> inputs);

    protected void ack(List<Tuple> inputs) throws Exception {
        inputs.forEach(collector::ack);
    }

    protected class BatchThread extends Thread {

        int batchMaxSize;
        boolean stopRequested = false;

        BatchThread(int batchMaxSize) {
            super("batch-bolt-thread");
            super.setDaemon(true);
            this.batchMaxSize = batchMaxSize;
        }

        @Override
        public void run() {
            while (!stopRequested) {
                try {
                    ArrayList<Tuple> batch = new ArrayList<>();
                    // drainTo() does not block, take() does.
                    Tuple t = queue.take();
                    batch.add(t);
                    if (batchMaxSize > 0) {
                        queue.drainTo(batch, batchMaxSize);
                    } else {
                        queue.drainTo(batch);
                    }
                    executeBatch(batch);

                } catch (InterruptedException e) {
                    getExceptionHandler().onException(e, collector, logger);
                }
            }
        }

        void stopRunning() {
            this.stopRequested = true;
        }
    }

}
