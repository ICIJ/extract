package org.icij.extract.http;

import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;

/**
 * Implements {@link HttpRequestRetryHandler} for retrying HTTP requests up to the given number of maximum attempts.
 *
 * Requests are only retried when no response is received from the server and not for any other kind of explicit,
 * permanent error.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class CountdownHttpRequestRetryHandler implements HttpRequestRetryHandler {

	public static final int DEFAULT_RETRIES = 3;

	private int countdown;

	public CountdownHttpRequestRetryHandler() {
		this(DEFAULT_RETRIES);
	}

	public CountdownHttpRequestRetryHandler(final int retries) {
		countdown = retries;
	}

	public boolean retryRequest(IOException exception, int executionCount, final HttpContext context) {
		return (exception instanceof NoHttpResponseException) && countdown-- > 0;
	}
}
