package org.icij.extract.core;

import java.io.InputStream;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
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
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.io.FilenameUtils;

import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpClientConfigurer;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class SolrUtils {

	public static void pinCertificate(final String trustStorePath) throws RuntimeException {
		try {
			pinCertificate(createTrustStore(trustStorePath));
		} catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException | KeyManagementException e) {
			throw new RuntimeException("Unable to pin certificate: " + trustStorePath + ".", e);
		}
	}

	private static void pinCertificate(final KeyStore trustStore) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
		final TrustManagerFactory trustManager = TrustManagerFactory.getInstance("X509");
		final SSLContext context = SSLContext.getInstance("TLS");

		trustManager.init(trustStore);
		context.init(null, trustManager.getTrustManagers(), null);

		HttpClientUtil.setConfigurer(new HttpClientConfigurer() {

			@Override
			public void configure(DefaultHttpClient httpClient, SolrParams config) {
				super.configure(httpClient, config);

				final SSLSocketFactory socketFactory = new SSLSocketFactory(context);

				socketFactory.setHostnameVerifier(SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);

				final SchemeRegistry registry = httpClient.getConnectionManager().getSchemeRegistry();

				// Make sure there's no way for a client to use plain HTTP.
				registry.unregister("http");
				registry.register(new Scheme("https", 443, socketFactory));
			}
		});
	}

	private static KeyStore createTrustStore(final String trustStorePath) throws IOException, NoSuchAlgorithmException, CertificateException, KeyStoreException {
		final String trustStoreExtension = FilenameUtils.getExtension(trustStorePath).toUpperCase();
		final String trustStoreType;

		// Key store types are defined in Oracle's Cryptography Standard Algorithm Name Documentation:
		// http://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#KeyStore
		if (trustStoreExtension.equals("P12")) {
			trustStoreType = "PKCS12";
		} else {
			trustStoreType = KeyStore.getDefaultType();
		}

		final KeyStore trustStore = KeyStore.getInstance(trustStoreType);
		final InputStream inputStream = new BufferedInputStream(new FileInputStream(trustStorePath));

		if (trustStoreExtension.equals("PEM") || trustStoreExtension.equals("DER")) {
			final X509Certificate certificate = (X509Certificate) CertificateFactory.getInstance("X.509")
				.generateCertificate(inputStream);

			// Create an empty key store.
			// This operation should never throw an exception.
			trustStore.load(null, null);
			trustStore.setCertificateEntry(Integer.toString(1), certificate);
		} else {
			trustStore.load(inputStream, "".toCharArray());
		}

		return trustStore;
	}
}
