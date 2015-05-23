package org.icij.extract;

import java.lang.Thread;
import java.lang.Runnable;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.ParseException;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class Main {
	private static final Logger logger = Logger.getLogger("extract");

	public static void main(String[] args) {
		int status = 0;
		final MainCli main = new MainCli(logger);

		try {
			main.parse(args);
		} catch (ParseException e) {
			logger.log(Level.SEVERE, "Failed to parse command line arguments: " + e.getMessage());
			status = 1;
		} catch (IllegalArgumentException e) {
			logger.log(Level.SEVERE, "Invalid command line argument: " + e.getMessage());
			status = 2;
		} catch (Throwable e) {
			logger.log(Level.SEVERE, "There was an error while executing.", e);
			status = 3;
		}

		System.exit(status);
	}
}
