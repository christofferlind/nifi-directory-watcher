package org.apache.nifi.processors.standard.additions.events;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.nifi.processors.standard.additions.TriConsumer;

public class EventMerger<T> implements AutoCloseable {
	
	private ThreadPoolExecutor executor;
	
	private final Object lock = new Object();
			
	private Map<String, TimedRunnable > cache = new HashMap<>(100);

	private ThreadGroup group;

	private TriConsumer<String, String, Collection<T>> eventConsumer;
	private Consumer<Throwable> onError = null;

	private long eventWaitTimeout = 100l;
	private int checkTimeoutSleep = 89;
	
	private Duration totalMaxWait = null;  

	public EventMerger(ThreadGroup group, int threadCount, TriConsumer<String, String, Collection<T>> eventConsumer) {
		Objects.requireNonNull(group);
		Objects.requireNonNull(eventConsumer);
		
		this.group = group;
		this.eventConsumer = eventConsumer;
		AtomicLong counter = new AtomicLong(0);
        executor = new ThreadPoolExecutor(
        		threadCount, 
        		threadCount,
                0L, 
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
        
        executor.setThreadFactory(r ->  {
        	Thread thread = new Thread(group, r, "EventMerger" + counter.incrementAndGet());
        	thread.setDaemon(true);
        	return thread;
        });
	}
	
	public void notify(String propName, String key, T event) {
		if(executor.isShutdown())
			throw new IllegalStateException("EventMerger is shutdown!");
		
		TimedRunnable runnable = null;
		
		synchronized (lock) {
			String cacheKey = propName + key;
			TimedRunnable r = cache.get(cacheKey);
			if(r != null) {
				r.events.add(event);
				r.timeout.set(Instant.now().plus(eventWaitTimeout, ChronoUnit.MILLIS));
			} else {
				runnable = new TimedRunnable();
				runnable.propName = propName;
				runnable.events.add(event);
				runnable.timeout.set(Instant.now().plus(eventWaitTimeout, ChronoUnit.MILLIS));
				runnable.onStartConsumingMessages = () -> {
					synchronized (lock) {
						cache.remove(cacheKey);
					}
				};
				
				runnable.key = cacheKey;
				cache.put(cacheKey, runnable);
			}
		}
		
		if(runnable != null) {
			executor.execute(runnable);
		}
	}
	
	public void setEventWaitTimeout(long eventWaitTimeout) {
		this.eventWaitTimeout = eventWaitTimeout;
	}
	
	public long getEventWaitTimeout() {
		return eventWaitTimeout;
	}
	
	public int getCheckTimeoutSleep() {
		return checkTimeoutSleep;
	}
	
	public void setCheckTimeoutSleep(int checkTimeoutSleep) {
		this.checkTimeoutSleep = checkTimeoutSleep;
	}
	
	public void setTotalMaxWait(Duration totalMaxWait) {
		this.totalMaxWait = totalMaxWait;
	}
	
	public Duration getTotalMaxWait() {
		return totalMaxWait;
	}
	
	public ThreadPoolExecutor getExecutor() {
		return executor;
	}
	
	@Override
	public void close() throws Exception {
		List<Runnable> list = executor.shutdownNow();
		group.interrupt();
		
		if(list != null) {
			//TODO: log
		}
	}
	
	class TimedRunnable implements Runnable {
		private AtomicReference<Instant> timeout = new AtomicReference<>();
		private Collection<T> events = Collections.synchronizedCollection(new LinkedList<>());
		private Runnable onStartConsumingMessages;

		private String propName;
		private String key;

		@Override
		public void run() {
			try {
				Instant started = Instant.now();
				while(!Thread.currentThread().isInterrupted()) {
					Instant currentTimeout = timeout.get();

					Instant now = Instant.now();
					if(now.isBefore(currentTimeout)) {
						if(totalMaxWait != null && now.isAfter(started.plus(totalMaxWait))) {
							//Don't sleep and process the accumulated batch
						} else {
							Thread.sleep(checkTimeoutSleep);
							continue;
						}
					}

					onStartConsumingMessages.run();

					try {
					eventConsumer.accept(propName, key, events);
					} catch (Throwable e) {
						e.printStackTrace();
					}
					return;
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			} catch (Throwable e) {
				if(onError != null)
					onError.accept(e);
				else
					e.printStackTrace();
			}
		}
	}
	
	public void setOnError(Consumer<Throwable> onError) {
		this.onError = onError;
	}
}
