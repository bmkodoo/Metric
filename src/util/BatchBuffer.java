package util;

import java.util.ArrayList;
import java.util.Collection;

public class BatchBuffer<T> {

    private final int bufferSize;
    private final Consumer<T> consumer;
    private Collection<T> batch;

    public BatchBuffer(int bufferSize, Consumer<T> consumer) {
        this.bufferSize = bufferSize;
        this.consumer = consumer;
        batch = buildBatch();
    }

    public void add(T e) {
        batch.add(e);
        if (batch.size() == bufferSize) {
            flush();
        }
    }

    public void flush() {
        consumer.consume(batch);
        batch = buildBatch();
    }

    protected Collection<T> buildBatch() {
        return new ArrayList<T>(bufferSize);
    }

}
