package org.icij.extract.test;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.BeforeClass;

public abstract class TestBase {

	protected static final Logger logger = Logger.getLogger("extract-test");

	@BeforeClass
	public static void setUpBeforeClass() {
		logger.setLevel(Level.INFO);
	}
}
