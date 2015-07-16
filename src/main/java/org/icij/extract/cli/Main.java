package org.icij.extract.cli;

import org.icij.extract.core.*;

import java.lang.Thread;
import java.lang.Runnable;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.LogManager;

import org.apache.commons.cli.ParseException;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class Main {

	// This bodge allows the logger to continue logging during the shutdown hook.
	// http://stackoverflow.com/a/13825590/821334
	static {
		System.setProperty("java.util.logging.manager", BodgeLogManager.class.getName());
	}

	public static class BodgeLogManager extends LogManager {
		static BodgeLogManager instance;

		public BodgeLogManager() {
			instance = this;
		}

		@Override
		public void reset() {}

		private void reset0() {
			super.reset();
		}

		public static void resetFinally() {
			instance.reset0();
		}
	}

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
		} catch (RuntimeException e) {
			logger.log(Level.SEVERE, "There was an error while executing.", e);
			status = 3;
		}

		System.exit(status);
	}
}
