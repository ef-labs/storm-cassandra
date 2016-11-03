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
package com.hmsonline.storm.cassandra.exceptions;

import com.datastax.driver.core.exceptions.OverloadedException;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.topology.FailedException;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.HashSet;

public class MappedExceptionHandler implements ExceptionHandler {

    private static final long serialVersionUID = 1L;

    public enum Action{
        DEFAULT,
        LOG,
        RETRY_TUPLE,
        ABANDON_TUPLE,
        KILL_WORKER
    }

    private HashSet<Class<? extends Exception>> retryTupleExceptions = new HashSet<>();
    private HashSet<Class<? extends Exception>> abandonTupleExceptions = new HashSet<>();
    private HashSet<Class<? extends Exception>> killWorkerExceptions = new HashSet<>();

    private Action defaultAction = Action.LOG;

    public MappedExceptionHandler() {
        setDefaults();
    }

    protected void setDefaults() {
        dieOn(CriticalException.class);
        abandonOn(UnrecoverableException.class);
        retryOn(RecoverableException.class);
        // Timeout on aquiring some lock, unlikely to be of real consequence.
        retryOn(InterruptedException.class);
        // Cassandra is overloaded, should be temporary
        retryOn(OverloadedException.class);
    }

    @Override
    @SuppressWarnings("Duplicates")
    public void onException(Exception e, IErrorReporter collector, Logger logger) {
        switch (actionFor(e)) {

            case KILL_WORKER:
                logger.warn("Killing worker/executor based on registered exception " + e.getClass().getName()
                        + ", Message: '" + e.getMessage() + "'", e);
                // wrap in RuntimeException in case FailedException has been
                // explicitly registered.
                throw new RuntimeException(e);

            case ABANDON_TUPLE:
                logger.info(
                        "Reporting error based on registered exception " + e.getClass().getName() + ", Message: '"
                                + e.getMessage() + "'", e);
                if(collector != null){
                    collector.reportError(e);
                }
                break;

            case RETRY_TUPLE:
                logger.info("Failing tuple based on registered exception " + e.getClass().getName() + ", Message: '"
                        + e.getMessage() + "'", e);
                throw new FailedException(e);

            default:
                logger.warn("Ignoring exception " + e.getClass().getName() + " and acknowledging batch. Message: '"
                        + e.getMessage() + "'", e);
                break;

        }
    }

    protected final void setDefaultAction(Action action){
        if (action == Action.DEFAULT) {
            this.defaultAction = Action.LOG;
        }
        else {
            this.defaultAction = action;
        }
    }

    protected final void addRetryOn(Class<? extends Exception> e) {
        abandonTupleExceptions.remove(e);
        killWorkerExceptions.remove(e);
        this.retryTupleExceptions.add(e);
    }

    protected final void addAbandonOn(Class<? extends Exception> e) {
        retryTupleExceptions.remove(e);
        killWorkerExceptions.remove(e);
        this.abandonTupleExceptions.add(e);
    }

    protected final void addDieOn(Class<? extends Exception> e) {
        retryTupleExceptions.remove(e);
        abandonTupleExceptions.remove(e);
        this.killWorkerExceptions.add(e);
    }

    public final Action actionFor(Exception e) {
        Class<? extends Exception> c = e.getClass();

        // First check explicit mappings
        if (this.killWorkerExceptions.contains(c)) {
            return Action.KILL_WORKER;
        } else if (this.abandonTupleExceptions.contains(c)) {
            return Action.ABANDON_TUPLE;
        } else if (this.retryTupleExceptions.contains(c)) {
            return Action.RETRY_TUPLE;
        }

        // Handle multiexceptions
         if (e instanceof MultiException) {
            MultiException me = (MultiException) e;
            return me.getExceptions()
                    .stream()
                    .map(this::actionFor)
                    .max((a1, a2) -> Integer.compare(a1.ordinal(), a2.ordinal()))
                    .orElse(defaultAction);
        }

        // None of the above, so return default
        return defaultAction;
    }

    @SafeVarargs
    public final MappedExceptionHandler retryOn(Class<? extends Exception>... exceptions) {
        Arrays.stream(exceptions)
                .forEach(this::addRetryOn);
        return this;
    }

    @SafeVarargs
    public final MappedExceptionHandler dieOn(Class<? extends Exception>... exceptions) {
        Arrays.stream(exceptions)
                .forEach(this::addDieOn);
        return this;
    }

    @SafeVarargs
    public final MappedExceptionHandler abandonOn(Class<? extends Exception>... exceptions) {
        Arrays.stream(exceptions)
                .forEach(this::addAbandonOn);
        return this;
    }

    public MappedExceptionHandler withDefaultAction(Action action) {
        setDefaultAction(action);
        return this;
    }

    public Action getDefaultAction() {
        return defaultAction;
    }

}
