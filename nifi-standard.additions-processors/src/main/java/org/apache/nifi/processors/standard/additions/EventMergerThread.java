package org.apache.nifi.processors.standard.additions;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class EventMergerThread<T> extends Thread {
	private Map<String, LimitedQueue<T>> cacheEvents = new ConcurrentHashMap<>();
	private Map<String, String> propMap = new WeakHashMap<String, String>();
	private TriConsumer<String, String, List<T>> eventConsumer;
	private int maxEventAge = 1_000;
	
	public EventMergerThread(ThreadGroup group, TriConsumer<String, String, List<T>> eventConsumer) {
		super(group, EventMergerThread.class.getSimpleName());
		this.eventConsumer = eventConsumer;
		setDaemon(true);
	}
	
	public void addEvent(String propName, String eventKey, T watchEvent) {
		propMap.put(eventKey, propName);
		LimitedQueue<T> queue = cacheEvents.get(eventKey);
		if(queue == null) {
			queue = new LimitedQueue<T>(5, Duration.of(maxEventAge, ChronoUnit.MILLIS));
			cacheEvents.put(eventKey, queue);
		}
		
		queue.add(watchEvent);
	}
	
	@Override
	public void run() {
		try {
			while(!isInterrupted()) {
				Thread.sleep(137);
				
				if(cacheEvents.isEmpty()) {
					Thread.sleep(500);
					continue;
				}
				
				Instant now = Instant.now();
				Set<Entry<String, LimitedQueue<T>>> collect = cacheEvents
							.entrySet()
							.stream()
							.filter(e -> now.isAfter(e.getValue().getMaxAge()))
							.collect(Collectors.toSet());

				if(collect == null || collect.isEmpty())
					continue;
				
				for (Entry<String, LimitedQueue<T>> entry : collect) {
					String key = entry.getKey();
					LimitedQueue<T> queue = entry.getValue();
					
					if(now.isBefore(queue.getMaxAge())) {
						continue;
					}
					
					cacheEvents.remove(key);
					
					eventConsumer.accept(propMap.get(key), key, queue);
				}
			}
			
		} catch (InterruptedException e) {
			this.interrupt();
			//Quit...
		}

		if(cacheEvents.size() > 0) {
			Set<Entry<String,LimitedQueue<T>>> set = cacheEvents.entrySet();
			for (Entry<String, LimitedQueue<T>> entry : set) {
				String key = entry.getKey();
				LimitedQueue<T> queue = entry.getValue();
				
				cacheEvents.remove(key);
				
				eventConsumer.accept(propMap.get(key), key, queue);
			}
		}
	}

	public void setMaxEventAge(int maxEventAge) {
		this.maxEventAge = maxEventAge;
	}

}
