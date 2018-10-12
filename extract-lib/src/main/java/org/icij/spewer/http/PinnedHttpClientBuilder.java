package org.icij.spewer.http;

import org.apache.commons.io.FilenameUtils;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManagerFactory;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Locale;

/**
 * Extends {@link HttpClientBuilder} with the ability to pin a certificate and a hostname.
 */
public class PinnedHttpClientBuilder extends HttpClientBuilder {

	private HostnameVerifier hostnameVerifier = null;
	private SSLContext sslContext = null;

	public static PinnedHttpClientBuilder createWithDefaults() {
		final PinnedHttpClientBuilder builder = new PinnedHttpClientBuilder();

		builder
			.setMaxConnPerRoute(32)
			.setMaxConnTotal(128)
			.disableRedirectHandling()
			.setRetryHandler(new CountdownHttpRequestRetryHandler());

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
			final InputStream input = new BufferedInputStream(new FileInputStream(trustStorePath))
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
}
