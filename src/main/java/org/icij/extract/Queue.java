package org.icij.extract;

import java.lang.Runtime;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.function.Supplier;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CompletableFuture;

import java.io.FileNotFoundException;
import java.io.IOException;

import java.nio.file.Path;

import org.apache.tika.exception.TikaException;
import org.apache.tika.exception.EncryptedDocumentException;

import org.xml.sax.SAXException;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class Queue {
	private final Logger logger;
	private final Consumer consumer;
	private final ExecutorService executor;

	public static final int DEFAULT_THREADS = Runtime.getRuntime().availableProcessors();

	public Queue(Logger logger, Consumer consumer, int threads) {
		this.logger = logger;
		this.consumer = consumer;
		this.executor = Executors.newFixedThreadPool(threads);
	}

	public Queue(Logger logger, Consumer consumer) {
		this(logger, consumer, DEFAULT_THREADS);
	}

	public void queue(final Path file) {
		CompletableFuture.supplyAsync(new Supplier<Path>() {

			@Override
			public Path get() {
				try {
					consumer.consume(file);
				} catch (FileNotFoundException e) {
					logger.log(Level.WARNING, "File not found: " + file + ". Skipping.", e);
				} catch (IOException e) {
					logger.log(Level.WARNING, "The document stream could not be read: " + file + ". Skipping.", e);
				} catch (EncryptedDocumentException e) {
					logger.log(Level.WARNING, "Skipping encrypted file: " + file + ".", e);
				} catch (TikaException e) {
					logger.log(Level.WARNING, "The document could not be parsed: " + file + ". Skipping.", e);
				} catch (IllegalArgumentException e) {
					logger.log(Level.SEVERE, e.toString());
				}

				return file;
			}
		}, executor);		
	}

	public void end() {
		executor.shutdown();
	}
}
