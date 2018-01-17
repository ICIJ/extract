package org.icij.time;

import org.junit.Assert;
import org.junit.Test;

import java.time.format.DateTimeParseException;

import static org.icij.time.HumanDuration.parse;

public class HumanDurationTest {

	@Test
	public void testParse() {
		Assert.assertEquals(500, parse("500ms").toMillis());
		Assert.assertEquals(60, parse("60s").getSeconds());
		Assert.assertEquals(60, parse("60m").toMinutes());
		Assert.assertEquals(1, parse("1h").toHours());
		Assert.assertEquals(1, parse("1d").toDays());
	}

	@Test
	public void testParseDefaultsToMilliseconds() {
		Assert.assertEquals(500, parse("500").toMillis());
	}

	@Test(expected = DateTimeParseException.class)
	public void testParseThrowsExceptionForInvalidValue() {
		parse("ms");
	}

	@Test(expected = DateTimeParseException.class)
	public void testParseThrowsExceptionForInvalidUnit() {
		parse("500y");
	}
}
