package org.icij.extract.http;

import java.util.Locale;

import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.KeyManagementException;
import java.security.UnrecoverableKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.CertificateException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.io.FilenameUtils;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.params.HttpParams;
import org.apache.http.params.SyncBasicHttpParams;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.protocol.HttpContext;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;

/**
 * Extends {@link HttpClientBuilder} with the ability to pin a certificate
 * and a hostname.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class PinnedHttpClientBuilder extends HttpClientBuilder {

	private HostnameVerifier hostnameVerifier = null;
	private SSLContext sslContext = null;

	/**
	 * Consume and block until the queue is drained.
	 *
	 * It's up to the user to stop the consumer if the thread is
	 * interrupted.
	 *
	 * @return Whether draining completed successfully or was stopped.
	 * @throws InterruptedException if interrupted while draining
	 */
	public static PinnedHttpClientBuilder createWithDefaults() {
		final PinnedHttpClientBuilder builder = new PinnedHttpClientBuilder();

		builder
			.setMaxConnPerRoute(32)
			.setMaxConnTotal(128)
			.disableRedirectHandling();

		return builder;
	}

	public PinnedHttpClientBuilder() {
		super();
	}

	public PinnedHttpClientBuilder setVerifyHostname(final String verifyHostname) {
		if (null == verifyHostname) {
			hostnameVerifier = null;
			return this;
		} else if (verifyHostname.equals("*")) {
			hostnameVerifier = NoopHostnameVerifier.INSTANCE;
		} else {
			hostnameVerifier = new BodgeHostnameVerifier(verifyHostname);
		}

		return this;
	}

	public PinnedHttpClientBuilder pinCertificate(final String trustStorePath) throws RuntimeException {
		return pinCertificate(trustStorePath, "");
	}

	public PinnedHttpClientBuilder pinCertificate(final String trustStorePath, final String trustStorePassword)
		throws RuntimeException {
		if (null != trustStorePath) {
			try {
				final TrustManagerFactory trustManager = TrustManagerFactory.getInstance("X509");

				trustManager.init(createTrustStore(trustStorePath, trustStorePassword));

				sslContext = SSLContext.getInstance("TLS");
				sslContext.init(null, trustManager.getTrustManagers(), null);
			} catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException
				| KeyManagementException e) {
				throw new RuntimeException("Unable to pin certificate: " + trustStorePath + ".", e);
			}
		} else {
			sslContext = null;
		}

		return this;
	}

	public CloseableHttpClient build() {
		if (null != hostnameVerifier) {
			super.setSSLHostnameVerifier(hostnameVerifier);
		}

		if (null != sslContext) {
			super.setSSLSocketFactory(new SSLConnectionSocketFactory(sslContext, hostnameVerifier));
		}

		return super.build();
	}

	public CloseableHttpClient buildConcurrentCompatible() {
		return new ConcurrentCompatibileClient(build());
	}

	public static KeyStore createTrustStore(final String trustStorePath, final String trustStorePassword)
		throws IOException, NoSuchAlgorithmException, CertificateException, KeyStoreException {

		final String trustStoreExtension = FilenameUtils.getExtension(trustStorePath).toUpperCase(Locale.ROOT);
		final String trustStoreType;

		// Key store types are defined in Oracle's Cryptography Standard Algorithm Name Documentation:
		// http://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#KeyStore
		if (trustStoreExtension.equals("P12")) {
			trustStoreType = "PKCS12";
		} else {
			trustStoreType = KeyStore.getDefaultType();
		}

		final KeyStore trustStore = KeyStore.getInstance(trustStoreType);

		try (
			final InputStream input = new BufferedInputStream(new FileInputStream(trustStorePath));
		) {
			if (trustStoreExtension.equals("PEM") || trustStoreExtension.equals("DER")) {
				final X509Certificate certificate = (X509Certificate) CertificateFactory.getInstance("X.509")
					.generateCertificate(input);

				// Create an empty key store.
				// This operation should never throw an exception.
				trustStore.load(null, null);
				trustStore.setCertificateEntry(Integer.toString(1), certificate);
			} else {
				trustStore.load(input, trustStorePassword.toCharArray());
			}
		}

		return trustStore;
	}

	public static class BodgeHostnameVerifier implements HostnameVerifier {

		private static final HostnameVerifier defaultVerifier = new DefaultHostnameVerifier();
		private final String verifyHostname;

		public BodgeHostnameVerifier(final String verifyHostname) {
			super();
			this.verifyHostname = verifyHostname;
		}

		@Override
		public final boolean verify(final String host, final SSLSession session) {
			return defaultVerifier.verify(verifyHostname, session);
		}
	}

	public static class ConcurrentCompatibileClient extends CloseableHttpClient {

		private final CloseableHttpClient client;

		public ConcurrentCompatibileClient(final CloseableHttpClient client) {
			super();
			this.client = client;
		}

		@Override
		public CloseableHttpResponse doExecute(final HttpHost target, final HttpRequest request,
			final HttpContext context) throws IOException, ClientProtocolException {
			throw new IllegalStateException("This method should never be called.");
		}

		@Override
		public ClientConnectionManager getConnectionManager() {
			return client.getConnectionManager();
		}

		@Override
		public HttpParams getParams() {
			return new SyncBasicHttpParams();
		}

		@Override
		public CloseableHttpResponse execute(final HttpHost target, final HttpRequest request)
			throws IOException, ClientProtocolException {
			return client.execute(target, request);
		}

		@Override
		public CloseableHttpResponse execute(final HttpHost target, final HttpRequest request,
			final HttpContext context) throws IOException, ClientProtocolException {
			return client.execute(target, request, context);
		}

		@Override
		public <T> T execute(final HttpHost target, final HttpRequest request,
			final ResponseHandler<? extends T> responseHandler) throws IOException, ClientProtocolException {
			return client.execute(target, request, responseHandler);
		}

		@Override
		public <T> T execute(final HttpHost target, final HttpRequest request,
			final ResponseHandler<? extends T> responseHandler, final HttpContext context)
			throws IOException, ClientProtocolException {
			return client.execute(target, request, responseHandler, context);
		}

		@Override
		public CloseableHttpResponse execute(final HttpUriRequest request)
			throws IOException, ClientProtocolException {
			return client.execute(request);
		}

		@Override
		public CloseableHttpResponse execute(final HttpUriRequest request, final HttpContext context)
			throws IOException, ClientProtocolException {
			return client.execute(request, context);
		}

		@Override
		public <T> T execute(final HttpUriRequest request, final ResponseHandler<? extends T> responseHandler)
			throws IOException, ClientProtocolException {
			return client.execute(request, responseHandler);
		}

		@Override
		public <T> T execute(final HttpUriRequest request, final ResponseHandler<? extends T> responseHandler,
			final HttpContext context) throws IOException, ClientProtocolException {
			return client.execute(request, responseHandler, context);
		}

		@Override
		public void close() throws IOException {
			client.close();
		}
	}
}
