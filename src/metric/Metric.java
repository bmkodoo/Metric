package metric;

import util.BatchBuffer;
import util.Data;
import util.Record;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by pavelsilin on 03.10.15.
 */

public class Metric {

    private static final int BATCH_SIZE = 1024;
    private static final int THREAD_QUEUE_CAPACITY = 4 * Runtime.getRuntime().availableProcessors();
    private static final int CORE_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private static final int MAXIMUM_POOL_SIZE = CORE_POOL_SIZE * 2;
    private static final int COUNT = 1;
    private static final List POISON_PILL = Collections.EMPTY_LIST;

    private static final class FullyBlockingQueue<T> extends LinkedBlockingQueue<T> {
        private FullyBlockingQueue(int capacity) {
             super(capacity);
        }

        @Override
        public boolean offer(T e) {
           try {
               put(e);
               return true;
           } catch (InterruptedException ex) {
               throw new RuntimeException(ex);
           }
        }
    }

    private class MetricComparator implements Comparator<Character> {

        private char desiredBase;

        public MetricComparator(char desiredBase){
            this.desiredBase = desiredBase;
        }

        @Override
        public int compare(Character o1, Character o2) {
            return Math.abs(desiredBase - o1) - Math.abs(desiredBase - o2);
        }
    }

    public static void main(String[] args) {
        Data data = new Data();
        Metric metric = new Metric();
        for (String base: args) {

            long start = System.currentTimeMillis();
            metric.doWork(data, base.charAt(0));
            long stop = System.currentTimeMillis();

            System.out.println("Elapsed time: " + ((double)(stop - start))/1000 + " sec");
        }
    }

    private void doWork(Data data, char desiredBase) {

        CountDownLatch latch = new CountDownLatch(COUNT);
        Iterator<Record> iterator = data.iterator();
        AtomicInteger baseCount = new AtomicInteger(0);
        Consumer consumer = new Consumer(baseCount, latch, desiredBase);
        BatchBuffer<Record> batchBuffer = new BatchBuffer<>(BATCH_SIZE, consumer);

        while (iterator.hasNext()){
            Record read = iterator.next();
            batchBuffer.add(read);
        }
        batchBuffer.flush();
        consumer.consume(POISON_PILL);

        try {
            latch.await();
            consumer.getService().shutdown();
            consumer.getService().awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Number of " + desiredBase +  "-base: " + baseCount);

    }

    class Consumer extends AccountantStatistics implements util.Consumer<Record> {

        private ExecutorService service;
        private char desiredBase;
        private MetricComparator comparator;
        private AtomicInteger baseCount;
        private CountDownLatch latch;

        private Consumer(AtomicInteger baseCount, CountDownLatch latch, char desiredBase) {
            this.desiredBase = desiredBase;
            this.comparator = new MetricComparator(this.desiredBase);
            this.baseCount = baseCount;
            service = new ThreadPoolExecutor(CORE_POOL_SIZE, MAXIMUM_POOL_SIZE, 0L, TimeUnit.MILLISECONDS,
                    new FullyBlockingQueue<>(THREAD_QUEUE_CAPACITY));
            this.latch = latch;
        }

        public ExecutorService getService(){
           return this.service;
        }

        @Override
        public void consume(final Collection<Record> batch) {
            service.submit(new AccountantStatistics(batch, latch, comparator, baseCount, desiredBase));
        }

    }

    public class AccountantStatistics implements Runnable {


        private Collection<Record> batch;
        private char desiredBase;
        private CountDownLatch latch;
        private Comparator comparator;
        private AtomicInteger baseCount;

        public AccountantStatistics(Collection<Record> batch, CountDownLatch latch,
                                    Comparator comparator, AtomicInteger baseCount, char desiredBase){

            this.desiredBase = desiredBase;
            this.batch = batch;
            this.latch = latch;
            this.comparator = comparator;
            this.baseCount = baseCount;
        }

        public AccountantStatistics() {
        }

        @Override
        public void run() {

            if (batch.isEmpty()) {
                latch.countDown();
            }

            Iterator<Record> iterator = batch.iterator();
            int count = 0;

            while (iterator.hasNext()) {
                Record read = iterator.next();
                char[] seq = read.getRead();
                Character[] readSeq = new Character[seq.length];

                for (int i = 0; i < seq.length; i++) {
                    readSeq[i] = seq[i];
                }

                Arrays.parallelSort(readSeq, comparator);

                int index = 0;
                while (readSeq[index] == this.desiredBase) {
                    index++;
                    count++;
                }
            }
            baseCount.addAndGet(count);

        }
    }


}
