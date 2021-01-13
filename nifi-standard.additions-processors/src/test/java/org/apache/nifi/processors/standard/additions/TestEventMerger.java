package org.apache.nifi.processors.standard.additions;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.nifi.processors.standard.additions.events.EventMerger;
import org.junit.Test;

public class TestEventMerger {
	
    @Test
    public void testSingleKey() throws Exception {
    	int messageCount = 10;
    	Collection<String> sentEvents = IntStream
    			.range(0, messageCount)
    			.mapToObj(i -> String.format("t1-%d", i))
    			.collect(Collectors.toList());
    		
    	Collection<String> foundEvents = new ArrayList<>(messageCount);
    	AtomicInteger counter = new AtomicInteger(0);
    	
    	BiConsumer<String, Collection<String>> handleEvents = (key, collection) -> {
    		foundEvents.addAll(collection);
    		counter.incrementAndGet();
    	};
    	
    	try(EventMerger<String> merger = new EventMerger<String>(Thread.currentThread().getThreadGroup(), 1, handleEvents)){
    		for (String string : sentEvents) {
    			merger.notify("t1", string);
    			Thread.sleep(50);
			}
    		
    		Thread.sleep(500);
		};
		
		assertEquals(1, counter.get());
		assertArrayEquals(sentEvents.toArray(), foundEvents.toArray());
    }

    @Test
    public void testMultiKeys() throws Exception {
    	Collection<String> foundEvents = new ArrayList<>(10);
    	
    	AtomicInteger counter = new AtomicInteger(0);
    	
    	BiConsumer<String, Collection<String>> handleEvents = (key, collection) -> {
    		foundEvents.addAll(collection);
    		counter.incrementAndGet();
    	};
    	
    	try(EventMerger<String> merger = new EventMerger<String>(Thread.currentThread().getThreadGroup(), 1, handleEvents)){
    		merger.notify("t1", "t1-1");
    		Thread.sleep(50);
    		merger.notify("t1", "t1-2");
    		merger.notify("t2", "t2-1");
    		Thread.sleep(50);
    		merger.notify("t1", "t1-3");
    		merger.notify("t2", "t2-2");
    		Thread.sleep(500);
		};
		
		assertEquals(2, counter.get());
		assertArrayEquals(new String[] {"t1-1", "t1-2", "t1-3", "t2-1", "t2-2"}, foundEvents.toArray());
    }

    

    @Test
    public void testMultiKeysMultiThreads() throws Exception {
    	int events = 20;
    	List<String> foundEvents = new Vector<>();
    	AtomicInteger counter = new AtomicInteger(0);
    	
    	Object lock = new Object();
    	
    	BiConsumer<String, Collection<String>> handleEvents = (key, collection) -> {
    		synchronized (lock) {
    			foundEvents.addAll(collection);
    			int count = counter.incrementAndGet();
    			if(count == 1)
    				assertEquals("t3", key);
			}
    	};
    	
    	try(EventMerger<String> merger = new EventMerger<String>(Thread.currentThread().getThreadGroup(), 2, handleEvents)){
    		merger.setEventWaitTimeout(100);

    		//Constantly emit events
    		for (int i = 0; i < events; i++) {
    			merger.notify("t1", "t1-" + i);
    			Thread.sleep(30);

    			// In the middle, emit one event
    			if(i == events/2) {
    	    		merger.notify("t3", "t3-1");
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
    	
    	BiConsumer<String, Collection<Integer>> handleEvents = (key, collection) -> {
    		assertArrayEquals(expectedBatch, collection.toArray(new Integer[collection.size()]));
    		counter.incrementAndGet();
    	};
    	
    	AtomicReference<Throwable> errors = new AtomicReference<>(null);
    	
    	try(EventMerger<Integer> merger = new EventMerger<Integer>(Thread.currentThread().getThreadGroup(), 2, handleEvents)){
    		merger.setOnError(errors::set);
    		merger.setEventWaitTimeout(100);

    		for (int i = 0; i < events; i++) {
    			merger.notify("t1", i);
    			merger.notify("t2", i);
    			merger.notify("t3", i);
    			merger.notify("t4", i);
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
    	
    	Integer[] expectedBatch = new Integer[events];
    	for (int i = 0; i < expectedBatch.length; i++) {
			expectedBatch[i] = i;
		}
    	
    	AtomicInteger counter = new AtomicInteger(0);
    	
    	BiConsumer<String, Collection<Integer>> handleEvents = (key, collection) -> {
//    		System.out.println(collection.stream().map(i -> Integer.toString(i)).collect(Collectors.joining(",")));
    		assertEquals(5, collection.size());
    		counter.incrementAndGet();
    	};
    	
    	AtomicReference<Throwable> errors = new AtomicReference<>(null);
    	
    	try(EventMerger<Integer> merger = new EventMerger<Integer>(Thread.currentThread().getThreadGroup(), 1, handleEvents)){
    		merger.setOnError(errors::set);
    		merger.setEventWaitTimeout(300);
    		merger.setTotalMaxWait(Duration.of(500, ChronoUnit.MILLIS));

    		for (int i = 0; i < events; i++) {
    			merger.notify("t1", i);
    			Thread.sleep(110);
			}

    		Thread.sleep(200);
		};
		
		Throwable throwable = errors.get();
		if(throwable != null)
			throw throwable;
		
		assertEquals(4, counter.get());
    }
}
