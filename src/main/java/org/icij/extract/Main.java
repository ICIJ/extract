package org.icij.extract;

import java.lang.Thread;
import java.lang.Runnable;

import org.apache.commons.cli.ParseException;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class Main {
	public static void main(String[] args) {
		Cli cli = new Cli();
		Runnable job = null;

		try {
			job = cli.parse(args);
		} catch (ParseException e) {
			System.exit(1);
		}

		new Thread(job).start();
	}
}
