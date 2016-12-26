package org.icij.sql.concurrent;

public interface SQLQueueCodec<T> extends SQLCodec<T> {

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
	 * @return the valued, for example {@literal processed}
	 */
	String getProcessedStatus();
}
