package org.apache.nifi.processors.standard.additions;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;

public class LimitedQueue<E> extends LinkedList<E> {
	private static final long serialVersionUID = -4370141994539018257L;

	private int limit;

	private Duration maxDuration;
	private Instant maxAge;

    public LimitedQueue(int limit, Duration maxDuration) {
        this.limit = limit;
		this.maxDuration = maxDuration;
		updateMaxAge(maxDuration);
    }

    public boolean isOld() {
    	return Instant.now().isAfter(maxAge);
    }

    @Override
    public boolean add(E o) {
        boolean added = super.add(o);
        while (added && size() > limit) {
           super.remove();
        }
        
        updateMaxAge(maxDuration);
        return added;
    }
    
	private void updateMaxAge(Duration maxDuration) {
		this.maxAge = Instant.now().plus(maxDuration);
	}
	
	public Instant getMaxAge() {
		return maxAge;
	}
}