package org.apache.nifi.processors.standard.additions;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

public class TestEventMerger {
	
    @Test
    public void testSingleKey() throws Exception {
    	int messageCount = 10;
    	Collection<String> sentEvents = IntStream
    			.range(0, messageCount)
    			.mapToObj(i -> String.format("t1-%d", i))
    			.collect(Collectors.toList());
    		
    	AtomicInteger counter = new AtomicInteger(0);
    	Map<String, Integer> eventCounts = new LinkedHashMap<>(sentEvents.size());
    	
    	Consumer<DirectoryWatcherEvent<String>> handleEvents = event -> {
    		counter.incrementAndGet();
    		eventCounts.putAll(event.getCounter());
    	};
    	
    	try(EventMerger<String> merger = new EventMerger<String>(Thread.currentThread().getThreadGroup(), 1, handleEvents)){
    		for (String string : sentEvents) {
    			merger.notify("prop", "t1", string);
    			Thread.sleep(50);
			}
    		
    		Thread.sleep(500);
		};
		
		assertEquals(1, counter.get());
		assertArrayEquals(sentEvents.toArray(), eventCounts.keySet().toArray());
    }

    @Test
    public void testMultiKeys() throws Exception {
    	AtomicInteger counter = new AtomicInteger(0);
    	Map<String, Integer> eventCounts = new LinkedHashMap<>();
    	
    	Consumer<DirectoryWatcherEvent<String>> handleEvents = event -> {
    		counter.incrementAndGet();
    		eventCounts.putAll(event.getCounter());
    	};
    	
    	try(EventMerger<String> merger = new EventMerger<String>(Thread.currentThread().getThreadGroup(), 1, handleEvents)){
    		merger.notify("prop", "t1", "t1-1");
    		Thread.sleep(50);
    		merger.notify("prop", "t1", "t1-2");
    		merger.notify("prop", "t2", "t2-1");
    		Thread.sleep(50);
    		merger.notify("prop", "t1", "t1-3");
    		merger.notify("prop", "t2", "t2-2");
    		Thread.sleep(500);
		};
		
		assertEquals(2, counter.get());
		assertArrayEquals(new String[] {"t1-1", "t1-2", "t1-3", "t2-1", "t2-2"}, eventCounts.keySet().toArray());
    }

    

    @Test
    public void testMultiKeysMultiThreads() throws Exception {
    	int events = 20;
    	List<String> foundEvents = new Vector<>();
    	AtomicInteger counter = new AtomicInteger(0);
    	
    	Object lock = new Object();
    	
    	Consumer<DirectoryWatcherEvent<String>> handleEvents = event -> {
    		synchronized (lock) {
    			foundEvents.addAll(event.getCounter().keySet());
    			int count = counter.incrementAndGet();
    			if(count == 1)
    				assertEquals("t3", event.getKey());
			}
    	};
    	
    	try(EventMerger<String> merger = new EventMerger<String>(Thread.currentThread().getThreadGroup(), 2, handleEvents)){
    		merger.setEventWaitTimeout(100);

    		//Constantly emit events
    		for (int i = 0; i < events; i++) {
    			merger.notify("prop", "t1", "t1-" + i);
    			Thread.sleep(30);

    			// In the middle, emit one event
    			if(i == events/2) {
    	    		merger.notify("prop", "t3", "t3-1");
    			}
			}

    		Thread.sleep(200);
		};
		
		assertEquals(2, counter.get());
		
		assertEquals("t3-1", foundEvents.get(0));

		for (int i = 1; i < events; i++) {
			String element = foundEvents.get(i);
			if(!element.matches("t[12]-.*"))
				throw new AssertionError("Expected the first 6 to start with t[12] but got: " + element);
		}
    }

    @Test
    public void testEnsureBatches() throws Throwable {
    	int events = 20;
    	
    	Integer[] expectedBatch = new Integer[events];
    	for (int i = 0; i < expectedBatch.length; i++) {
			expectedBatch[i] = i;
		}
    	
    	AtomicInteger counter = new AtomicInteger(0);
    	
    	Consumer<DirectoryWatcherEvent<Integer>> handleEvents = event -> {
//    		assertArrayEquals(expectedBatch, collection.toArray(new Integer[collection.size()]));
    		counter.incrementAndGet();
    	};
    	
    	AtomicReference<Throwable> errors = new AtomicReference<>(null);
    	
    	try(EventMerger<Integer> merger = new EventMerger<Integer>(Thread.currentThread().getThreadGroup(), 2, handleEvents)){
    		merger.setOnError(errors::set);
    		merger.setEventWaitTimeout(100);

    		for (int i = 0; i < events; i++) {
    			merger.notify("prop", "t1", i);
    			merger.notify("prop", "t2", i);
    			merger.notify("prop", "t3", i);
    			merger.notify("prop", "t4", i);
    			Thread.sleep(30);
			}

    		Thread.sleep(200);
		};
		
		Throwable throwable = errors.get();
		if(throwable != null)
			throw throwable;
		
		assertEquals(4, counter.get());
    }

    @Test
    public void testMaxWait() throws Throwable {
    	int events = 20;
    	int expectedBatches = 4;
    	int expectedBatcheSize = events / expectedBatches;
    	int sleepBatch = 200;
    	
    	Integer[] expectedBatch = new Integer[events];
    	for (int i = 0; i < expectedBatch.length; i++) {
			expectedBatch[i] = i;
		}
    	
    	AtomicInteger counter = new AtomicInteger(0);
    	
    	Collection<Map<Integer, Integer>> counterEvents = new HashSet<>(expectedBatches);
    	
    	Consumer<DirectoryWatcherEvent<Integer>> handleEvents = event -> {
    		counter.incrementAndGet();
    		counterEvents.add(event.getCounter());
//    		System.out.println(event.getCounter());
    	};
    	
    	AtomicReference<Throwable> errors = new AtomicReference<>(null);
    	
    	try(EventMerger<Integer> merger = new EventMerger<Integer>(Thread.currentThread().getThreadGroup(), 1, handleEvents)){
    		int sleepEvent = sleepBatch / expectedBatcheSize;
    		int sleepEventTimeout = (int) (sleepBatch / 2);
    		int sleepTotal = (int) (sleepBatch * 1.1);
//    		System.out.println("Event sleep: " + sleepEvent);
//    		System.out.println("Batch sleep: " + sleepBatch);
//    		System.out.println("Event timeout: " + sleepEventTimeout);
//    		System.out.println("TotalMaxWait: " + sleepTotal);
    		
    		merger.setOnError(errors::set);
    		merger.setEventWaitTimeout(sleepEventTimeout);
    		merger.setTotalMaxWait(Duration.of(sleepTotal, ChronoUnit.MILLIS));

//    		Instant start = Instant.now();
    		for (int i = 0; i < events; i++) {
    			if(i % expectedBatcheSize == 0) {
//    				if(i!=0) System.out.println("Batch: " + i + ", " + Duration.between(start, Instant.now()).toMillis());
//    				start = Instant.now();
    				Thread.sleep(sleepBatch);
    			}

    			merger.notify("prop", "t1", i);
    			Thread.sleep(sleepEvent);
			}

    		Thread.sleep(sleepTotal);
		};
		
		Throwable throwable = errors.get();
		if(throwable != null)
			throw throwable;
		
		assertEquals(expectedBatches, counter.get());
		for (Map<Integer, Integer> entry : counterEvents) {
			assertEquals(expectedBatcheSize, entry.values().stream().mapToInt(Integer::valueOf).sum());
		}
    }
}
