package org.apache.nifi.processors.standard.additions;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.apache.nifi.logging.ComponentLog;

public class DirectoryWatcherThread extends Thread {
	private final Map<String, Path> paths;
	private final Kind<?>[] kinds;
	private final ComponentLog logger;
	
	private volatile boolean stopped = false;
	private BiConsumer<String, List<WatchEvent<?>>> eventConsumer;

	public DirectoryWatcherThread(
			ThreadGroup group, 
			Map<String, Path> paths, 
			Kind<?>[] kinds, 
			BiConsumer<String, List<WatchEvent<?>>> eventConsumer,
			ComponentLog logger) {
		
		Objects.requireNonNull(paths);
		Objects.requireNonNull(kinds);
		Objects.requireNonNull(logger);
		Objects.requireNonNull(eventConsumer);
		this.eventConsumer = eventConsumer;
		this.paths = paths;
		this.kinds = kinds;
		this.logger = logger;
		this.setDaemon(true);
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
				
				WatchKey key = path.register(watchService, kinds);
				keyMap.put(key, entry.getKey());

				debugMessage("Registered path: " + path);
			}
			
			while(!isInterrupted()) {
				if(stopped)
					return;
				
				WatchKey key = watchService.poll(3, TimeUnit.SECONDS);
				if(key == null) {
					continue;
				}

				try {
					String propName = keyMap.get(key);
					debugMessage("Got event on property: " + propName);
					eventConsumer.accept(propName, key.pollEvents());
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
