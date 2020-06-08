package org.apache.nifi.processors.standard.additions;

@FunctionalInterface
public interface TriConsumer<V1, V2, V3>  {
	public abstract void accept(V1 value1, V2 value2, V3 value3);
}
