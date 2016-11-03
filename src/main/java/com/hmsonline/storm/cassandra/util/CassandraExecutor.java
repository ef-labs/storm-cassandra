package com.hmsonline.storm.cassandra.util;

import com.datastax.driver.core.*;
import com.hmsonline.storm.cassandra.exceptions.MultiException;
import com.hmsonline.storm.cassandra.exceptions.RecoverableException;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Utility class for executing cassandra statements.
 */
public class CassandraExecutor {
    
    private final Session session;
    private final ExecutorService executor;
    private final Semaphore throttle;

    public CassandraExecutor(Session session, ExecutorService executor, Semaphore throttle) {
        this.session = session;
        this.executor = executor;
        this.throttle = throttle;
    }

    public CassandraExecutor(Session session, ExecutorService executor, Integer maxParallelism) {
        this.session = session;
        this.executor = executor;
        if (maxParallelism > 0) {
            throttle = new Semaphore(maxParallelism, false);
        }
        else {
            throttle = new Semaphore(Integer.MAX_VALUE, false);
        }
    }

    /**
     * Executes a prepared statement for a list of binding values, and calls a resulthandler after each has completed.
     * Exceptions are collected and a single {@link MultiException} is thrown at the end.
     * @param statement the prepared statement
     * @param bindings the list of values to bind
     * @param resultHandler the result handler, called once for each executed statement.
     */
    public void executeAndWait(
            PreparedStatement statement,
            List<List<Object>> bindings,
            BiConsumer<List<Object>, ResultSet> resultHandler)
    {
        CountDownLatch latch = new CountDownLatch(bindings.size());
        MultiException exceptions = new MultiException("Failed to execute statements");

        bindings.forEach(binding -> {

            try {
                throttle.acquire();
            } catch (InterruptedException e) {
                throw new RecoverableException("Failed to acquire throttle permit", e);
            }

            try {
                ResultSetFuture future = session.executeAsync(statement.bind(binding.toArray()));
                future.addListener(
                        () -> {
                            try {
                                ResultSet result = future.getUninterruptibly();
                                if (resultHandler != null) {
                                    resultHandler.accept(binding, result);
                                }
                            } catch (Exception e) {
                                exceptions.add(e);
                            } finally {
                                throttle.release();
                                latch.countDown();
                            }
                        },
                        executor);
            }
            catch (RuntimeException e) {
                throttle.release();
                throw e;
            }
        });

        try {
            latch.await();
        }
        catch (InterruptedException e) {
            exceptions.add(e);
        }

        if (exceptions.size() > 0) {
            throw exceptions;
        }

    }

    /**
     * Executes a prepared statement for a list of keys and statements, and calls a resulthandler with matched keys
     * and results after each has completed.
     * Exceptions are collected and a single {@link MultiException} is thrown at the end.
     * @param keys a list of values to associate with each statement (for result handling)
     * @param statements the list of statements to execute
     * @param resultHandler the result handler.
     */
    public <K> void executeAndWait(
            List<K> keys,
            List<BoundStatement> statements,
            BiConsumer<K, ResultSet> resultHandler)
    {
        CountDownLatch latch = new CountDownLatch(statements.size());
        MultiException exceptions = new MultiException("Failed to execute statements");
        for (int i = 0; i < statements.size(); i++) {
            final int finalI = i;

            try {
                throttle.acquire();
            } catch (InterruptedException e) {
                throw new RecoverableException("Failed to acquire throttle permit", e);
            }

            try {
                ResultSetFuture future = session.executeAsync(statements.get(finalI));
                future.addListener(
                        () -> {
                            try {
                                ResultSet result = future.getUninterruptibly();
                                if (resultHandler == null) {
                                    // Do nothing
                                } else if (keys == null) {
                                    resultHandler.accept(null, result);
                                } else {
                                    resultHandler.accept(keys.get(finalI), result);
                                }
                            } catch (Exception e) {
                                exceptions.add(e);
                            } finally {
                                throttle.release();
                                latch.countDown();
                            }
                        },
                        executor);
            }
            catch (RuntimeException e) {
                throttle.release();
                throw e;
            }
        }
        try {
            latch.await();
        }
        catch (InterruptedException e) {
            exceptions.add(e);
        }

        if (exceptions.size() > 0) {
            throw exceptions;
        }

    }

    public void executeAsync(
            Statement statement,
            Consumer<ResultSet> resultHandler) {

        ResultSetFuture resultFuture = session
                .executeAsync(statement);
        resultFuture.addListener(
                () -> {
                    if (resultHandler != null) {
                        resultHandler.accept(resultFuture.getUninterruptibly());
                    }
                },
                executor);
    }

    /**
     * Executes a prepared statement for a list of binding values, and calls a resulthandler after each has completed.
     * Exceptions are collected and a single {@link MultiException} is thrown at the end.
     */
    public void executeAsync(
            PreparedStatement statement,
            List<Object> binding,
            Consumer<ResultSet> resultHandler
    )
    {
        ResultSetFuture future = session.executeAsync(
                statement.bind(binding.toArray())
        );
        future.addListener(
                () -> {
                    if (resultHandler != null) {
                        resultHandler.accept(future.getUninterruptibly());
                    }},
                executor);
    }

    public PreparedStatement prepare(RegularStatement statement) {
        return session.prepare(statement);
    }

}
