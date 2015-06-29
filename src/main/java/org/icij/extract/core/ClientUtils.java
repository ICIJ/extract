package org.icij.extract.core;

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

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class ClientUtils {

	public static CloseableHttpClient createHttpClient(final String trustStorePath, final String verifyHostname) throws RuntimeException {
		final HttpClientBuilder clientBuilder = HttpClientBuilder.create();

		if (null != trustStorePath) {
			HostnameVerifier hostnameVerifier = null;
			SSLConnectionSocketFactory socketFactory = null;

			if (null != verifyHostname) {
				hostnameVerifier = createHostnameVerifier(verifyHostname);
			}

			try {
				socketFactory = createPinnedSocketFactory(trustStorePath, hostnameVerifier);
			} catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException | KeyManagementException e) {
				throw new RuntimeException("Unable to pin certificate: " + trustStorePath + ".", e);
			}

			clientBuilder.setSSLSocketFactory(socketFactory);
		}

		if (null != verifyHostname && null == trustStorePath) {
			clientBuilder.setSSLHostnameVerifier(createHostnameVerifier(verifyHostname));
		}

		return clientBuilder.setMaxConnPerRoute(32)
			.setMaxConnTotal(128)
			.disableRedirectHandling()
			.build();
	}

	private static HostnameVerifier createHostnameVerifier(final String verifyHostname) {	
		if (verifyHostname.equals("*")) {
			return NoopHostnameVerifier.INSTANCE;
		}

		final HostnameVerifier defaultVerifier = new DefaultHostnameVerifier();

		return new HostnameVerifier() {

			@Override
			public final boolean verify(final String host, final SSLSession session) {
				return defaultVerifier.verify(verifyHostname, session);
			}
		};
	}

	private static SSLConnectionSocketFactory createPinnedSocketFactory(final String trustStorePath, final HostnameVerifier hostnameVerifier) throws IOException, NoSuchAlgorithmException, KeyStoreException, CertificateException, KeyManagementException {
		return createPinnedSocketFactory(createTrustStore(trustStorePath, ""), hostnameVerifier);
	}

	private static SSLConnectionSocketFactory createPinnedSocketFactory(final KeyStore trustStore, final HostnameVerifier hostnameVerifier) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
		final SSLContext sslContext = SSLContext.getInstance("TLS");
		final TrustManagerFactory trustManager = TrustManagerFactory.getInstance("X509");

		trustManager.init(trustStore);
		sslContext.init(null, trustManager.getTrustManagers(), null);

		final SSLConnectionSocketFactory socketFactory = new SSLConnectionSocketFactory(sslContext, hostnameVerifier);

		return socketFactory;
	}

	private static KeyStore createTrustStore(final String trustStorePath, final String trustStorePassword) throws IOException, NoSuchAlgorithmException, CertificateException, KeyStoreException {

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
		final InputStream input = new BufferedInputStream(new FileInputStream(trustStorePath));

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

		input.close();

		return trustStore;
	}
}
