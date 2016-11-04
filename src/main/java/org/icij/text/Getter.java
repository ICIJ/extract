package org.icij.text;

import java.util.ResourceBundle;

public class Getter {

	private static ResourceBundle bundle = null;

	public static String t(final String key) {
		if (null == bundle) {
			load();
		}

		return bundle.getString(key);
	}

	private synchronized static void load() {
		bundle = ResourceBundle.getBundle("text");
	}
}
