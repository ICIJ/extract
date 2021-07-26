package org.icij.extract.mysql;

public interface SQLQueueCodec<E> extends SQLCodec<E> {

	/**
	 * Get the name of the column used to store the status value.
	 *
	 * @return the key name, for example {@literal status}
	 */
	String getStatusKey();

	/**
	 * Gets the status value for an object that's queued and waiting.
	 *
	 * @return the value, for example {@literal waiting}
	 */
	String getWaitingStatus();

	/**
	 * Gets the status value for an object that's been processed.
	 *
	 * @return the value, for example {@literal processed}
	 */
	String getProcessedStatus();
}
