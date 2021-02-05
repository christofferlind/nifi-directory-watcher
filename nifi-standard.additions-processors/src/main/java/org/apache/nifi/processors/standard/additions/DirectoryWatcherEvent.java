package org.apache.nifi.processors.standard.additions;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class DirectoryWatcherEvent<T> {
	
	private String property = null;
	
	private Map<T, Integer> counter = new LinkedHashMap<>(3);

	private String key;
	
	public DirectoryWatcherEvent(String property, String key) {
		setProperty(property);
		setKey(key);
	}

	public DirectoryWatcherEvent(String property, String key, Collection<T> c) {
		setProperty(property);
		setKey(key);
		
		if(c != null) {
			c.forEach(this::addEvent);
		}
	}

	public DirectoryWatcherEvent(String property, String key, T event) {
		setProperty(property);
		setKey(key);
		
		if(event != null)
			addEvent(event);
	}
	
	private void setKey(String key) {
		this.key = key;
	}
	
	public String getKey() {
		return key;
	}

	private void setProperty(String property) {
		this.property = property;
	}
	
	public void addEvent(T event) {
		counter.compute(event, (e, old) -> old == null ? 1 : old+1);
	}
	
	public String getProperty() {
		return property;
	}
	
	public Map<T, Integer> getCounter() {
		return Collections.unmodifiableMap(counter);
	}
}
