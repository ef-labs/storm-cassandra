package com.hmsonline.storm.cassandra.exceptions;


import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MultiException extends RuntimeException {

    private static final long serialVersionUID = -4975428774149613720L;

    private Queue<Exception> exceptions;

    public MultiException(String message) {
    }

    public MultiException(String message, Iterable<Exception> exceptions) {
        getQueue().forEach(this.exceptions::add);
    }

    public void add(Exception exception) {
        getQueue().add(exception);
    }

    public int size() {
        return exceptions == null
            ? 0
            : getQueue().size();
    }

    @Override
    public synchronized Exception getCause() {
        if (size() > 0) {
            return exceptions.peek();
        }
        else {
            return null;
        }
    }

    @Override
    public String toString() {
        if (exceptions == null) {
            return super.toString();
        }
        else {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString())
                .append(System.lineSeparator())
                .append("Multiple exceptions:")
                .append(System.lineSeparator());

            exceptions.forEach(Exception -> {
                sb.append(Exception.toString())
                        .append(System.lineSeparator());
            });
            return sb.toString();
        }
    }

    @Override
    public void printStackTrace(PrintStream s) {
        super.printStackTrace(s);
        if (exceptions != null) {
            exceptions.forEach(Exception -> Exception.printStackTrace(s));
        }
    }

    @Override
    public void printStackTrace(PrintWriter s) {
        super.printStackTrace(s);
        if (exceptions != null) {
            exceptions.forEach(Exception -> Exception.printStackTrace(s));
        }
    }

    private Queue<Exception> getQueue() {
        if (exceptions == null) {
            synchronized (this) {
                if (exceptions == null) {
                    exceptions = new ConcurrentLinkedQueue<>();
                }
            }
        }
        return exceptions;
    }

    public Queue<Exception> getExceptions() {
        return exceptions;
    }
}
