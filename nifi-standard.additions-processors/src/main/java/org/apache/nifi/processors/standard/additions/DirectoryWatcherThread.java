package org.apache.nifi.processors.standard.additions;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.standard.additions.events.EventMerger;

public class DirectoryWatcherThread extends Thread {
	private final Map<String, Path> paths;
	private final Collection<Kind<?>> kinds;
	private final ComponentLog logger;
	
	private volatile boolean stopped = false;
	
	private EventMerger<WatchEvent<?>> threadEventMerger = null;
	
	private final TriConsumer<String, String, Collection<WatchEvent<?>>> eventConsumer;

	public DirectoryWatcherThread(
			ThreadGroup group, 
			Map<String, Path> paths, 
			Collection<WatchEvent.Kind<?>> kinds,
			int maxEventAge,
			int maxWait,
			TriConsumer<String, String, Collection<WatchEvent<?>>> eventConsumer,
			ComponentLog logger) {
		
		Objects.requireNonNull(paths);
		Objects.requireNonNull(kinds);
		Objects.requireNonNull(logger);
		Objects.requireNonNull(eventConsumer);
		this.paths = paths;
		this.kinds = kinds;
		this.logger = logger;
		this.eventConsumer = eventConsumer;
		this.setDaemon(true);
		
		if(	kinds.contains(StandardWatchEventKinds.ENTRY_MODIFY) ||
			kinds.contains(StandardWatchEventKinds.ENTRY_CREATE)) {
			threadEventMerger = new EventMerger<WatchEvent<?>>(group, 2, this.eventConsumer);
			threadEventMerger.setEventWaitTimeout(maxEventAge);
			threadEventMerger.setTotalMaxWait(Duration.of(maxWait, ChronoUnit.MILLIS));
		}
	}
	
	@Override
	public void run() {
		debugMessage("Starting directory watcher");
		WatchService watchService = null;
		try {
			Map<WatchKey, String> keyMap = new HashMap<>(paths.size());

			watchService = FileSystems.getDefault().newWatchService();
			for (Entry<String, Path> entry : paths.entrySet()) {
				Path path = entry.getValue();
				
				WatchKey key = path.register(watchService, kinds.toArray(new WatchEvent.Kind[kinds.size()]));
				keyMap.put(key, entry.getKey());

				debugMessage("Registered path: " + path);
			}

			while(!isInterrupted()) {
				if(stopped)
					return;
				
				WatchKey key = watchService.poll(60, TimeUnit.SECONDS);
				if(key == null) {
					continue;
				}

				try {
					String propName = keyMap.get(key);
					debugMessage("Got event on property: " + propName);
					
					List<WatchEvent<?>> events = key.pollEvents();
					for (WatchEvent<?> watchEvent : events) {
						//If it is a modification event, try to merge it
						if(		StandardWatchEventKinds.ENTRY_MODIFY.equals(watchEvent.kind()) || 
								StandardWatchEventKinds.ENTRY_CREATE.equals(watchEvent.kind())) {
							String eventKey = watchEvent.context().toString();
							threadEventMerger.notify(propName, eventKey, watchEvent);
						} else {
							eventConsumer.accept(propName, watchEvent.kind().name(), Collections.singletonList(watchEvent));
						}
					}
				} catch (Throwable e) {
					logger.error(e.getMessage(), e);
				} finally {
					key.reset();
				}
			}
		} catch (InterruptedException e) {
			//Quit nicely
			interrupt();
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
		} finally {
			if(watchService != null) {
				try {
					debugMessage("Stopping filesystem watch service");
					watchService.close();
				} catch (Throwable e) {
					logger.error(e.getMessage(), e);
				}
			}
			
			if(threadEventMerger != null) {
				try {
					threadEventMerger.close();
					debugMessage("Closed event merger");
				} catch (Throwable e) {
				}
			}
		}
		
		debugMessage("Stopped directory watcher");
	}

	private void debugMessage(String string) {
		if(!logger.isDebugEnabled())
			return;
		
		logger.debug(string);
	}

	public void requestStop() {
		this.stopped = true;
	}
}
