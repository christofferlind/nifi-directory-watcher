package org.apache.nifi.processors.standard.additions;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class EventMergerThreadTest {
    
	@Test
    public void testSingleEvent() throws Exception {
		ThreadGroup group = Thread.currentThread().getThreadGroup();
		AtomicInteger counter = new AtomicInteger(0);
    	EventMergerThread<String> mergerThread = new EventMergerThread<>(group, (prop, key, e) -> counter.incrementAndGet());
    	mergerThread.setMaxEventAge(1_000);
    	mergerThread.start();
    	
    	mergerThread.addEvent("prop", "prop,eventType,/", "event1");
    	mergerThread.interrupt();
    	mergerThread.join();
    	
    	assertEquals(1, counter.get());
    }

	@Test
    public void testWaitForExpiration() throws Exception {
		ThreadGroup group = Thread.currentThread().getThreadGroup();
		AtomicInteger counter = new AtomicInteger(0);
    	EventMergerThread<String> mergerThread = new EventMergerThread<>(group, (prop, key, e) -> counter.incrementAndGet());
    	mergerThread.setMaxEventAge(100);
    	mergerThread.start();
    	
    	mergerThread.addEvent("prop", "prop,eventType,/", "event1");
    	Thread.sleep(500);
    	assertEquals(1, counter.get());
    	
    	mergerThread.interrupt();
    	mergerThread.join();
    }

	@Test
    public void testMergeEvents() throws Exception {
		ThreadGroup group = Thread.currentThread().getThreadGroup();
		AtomicInteger counter = new AtomicInteger(0);
    	EventMergerThread<String> mergerThread = new EventMergerThread<>(group, (prop, key,e) -> counter.incrementAndGet());
    	mergerThread.setMaxEventAge(300);
    	mergerThread.start();
    	
    	for (int i = 0; i < 100; i++) {
    		mergerThread.addEvent("prop", "prop,eventType,/tmp/file1.txt", "event" + Integer.toString(i));
		}
    	
    	assertEquals(0, counter.get());
    	Thread.sleep(500);
    	assertEquals(1, counter.get());
    	
    	mergerThread.interrupt();
    	mergerThread.join();
    }

	@Test
    public void testMultipleEvents() throws Exception {
		ThreadGroup group = Thread.currentThread().getThreadGroup();
		Map<String, AtomicInteger> counters = new ConcurrentHashMap<String, AtomicInteger>();
		counters.put("prop1", new AtomicInteger(0));
		counters.put("prop2", new AtomicInteger(0));
    	
		EventMergerThread<String> mergerThread = new EventMergerThread<>(group, 
    			(prop, key, e) -> counters.get(prop).incrementAndGet()
    			);
		
    	mergerThread.setMaxEventAge(300);
    	mergerThread.start();
    	
    	mergerThread.addEvent("prop1", "prop1,created,/tmp/file1.txt", "event1");
    	mergerThread.addEvent("prop2", "prop2,modified,/tmp/sub/file1.txt", "event2");
    	
    	mergerThread.interrupt();
    	mergerThread.join();

    	assertEquals(1, counters.get("prop1").get());
    	assertEquals(1, counters.get("prop2").get());
    }

	@Test
    public void testMultipleEvents2() throws Exception {
		ThreadGroup group = Thread.currentThread().getThreadGroup();
		Map<String, AtomicInteger> counters = new ConcurrentHashMap<String, AtomicInteger>();
		counters.put("prop1", new AtomicInteger(0));
		counters.put("prop2", new AtomicInteger(0));
    	
		EventMergerThread<String> mergerThread = new EventMergerThread<>(group, 
    			(prop, key, e) -> counters.get(prop).incrementAndGet()
    			);
		
    	mergerThread.setMaxEventAge(300);
    	mergerThread.start();
    	
    	mergerThread.addEvent("prop2", "prop2,modified,/", "event1");
    	mergerThread.addEvent("prop2", "prop2,modified,/", "event2");

    	Thread.sleep(100);
    	mergerThread.addEvent("prop1", "prop1,created,/", "event1");
    	Thread.sleep(100);
    	
    	mergerThread.addEvent("prop2", "prop2,modified,/", "event3");
    	mergerThread.addEvent("prop2", "prop2,modified,/", "event4");
    	
    	mergerThread.interrupt();
    	mergerThread.join();

    	assertEquals(1, counters.get("prop1").get());
    	assertEquals(1, counters.get("prop2").get());
    }

	@Test
    public void testMultipleEventsSameProperty() throws Exception {
		ThreadGroup group = Thread.currentThread().getThreadGroup();
		Map<String, AtomicInteger> counters = new ConcurrentHashMap<String, AtomicInteger>();
		counters.put("prop1", new AtomicInteger(0));
		counters.put("prop2", new AtomicInteger(0));
    	
		EventMergerThread<String> mergerThread = new EventMergerThread<>(group, 
    			(prop, key, e) -> counters.get(prop).incrementAndGet()
    			);
		
    	mergerThread.setMaxEventAge(300);
    	mergerThread.start();
    	
    	mergerThread.addEvent("prop2", "prop2,modified,/tmp/big.bin", "event1");
    	mergerThread.addEvent("prop2", "prop2,modified,/tmp/big.bin", "event2");
    	mergerThread.addEvent("prop2", "prop2,created,/tmp/test.txt", "event1");
    	mergerThread.addEvent("prop2", "prop2,modified,/tmp/big.bin", "event3");
    	
    	mergerThread.interrupt();
    	mergerThread.join();

    	assertEquals(0, counters.get("prop1").get());
    	assertEquals(2, counters.get("prop2").get());
    }

	@Test
    public void testMultipleEventsSameProperty2() throws Exception {
		ThreadGroup group = Thread.currentThread().getThreadGroup();
		Map<String, AtomicInteger> counters = new ConcurrentHashMap<String, AtomicInteger>();
		counters.put("prop,modification,/tmp/file1.txt", new AtomicInteger(0));
		counters.put("prop,modification,/tmp/file2.txt", new AtomicInteger(0));
    	
		EventMergerThread<String> mergerThread = new EventMergerThread<>(group, 
    			(prop, key, events) -> counters.get(key).incrementAndGet()
    			);
		
    	mergerThread.setMaxEventAge(300);
    	mergerThread.start();
    	
    	for (int i = 0; i < 100; i++) {
    		mergerThread.addEvent("prop", "prop,modification,/tmp/file1.txt", "event" + Integer.toString(i));
    	}
    	
    	assertEquals(0, counters.get("prop,modification,/tmp/file1.txt").get());
    	
    	Thread.sleep(200);
    	
    	for (int i = 0; i < 5; i++) {
    		mergerThread.addEvent("prop", "prop,modification,/tmp/file2.txt", "event" + Integer.toString(i));
        	Thread.sleep(100);
		}

    	assertEquals(1, counters.get("prop,modification,/tmp/file1.txt").get());
    	
    	mergerThread.interrupt();
    	mergerThread.join();

    	assertEquals(1, counters.get("prop,modification,/tmp/file1.txt").get());
    	assertEquals(1, counters.get("prop,modification,/tmp/file2.txt").get());
    }

}
