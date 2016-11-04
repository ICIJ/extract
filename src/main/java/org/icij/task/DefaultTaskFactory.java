package org.icij.task;

import java.util.HashMap;
import java.util.Set;

public class DefaultTaskFactory {
	private final HashMap<String, Class<? extends DefaultTask>> tasks;

	public DefaultTaskFactory() {
		tasks = new HashMap<>();
	}

	public void addTask(final String name, final Class<? extends DefaultTask> task) {
		tasks.put(name, task);
	}

	public DefaultTask getTask(final String name) throws Exception {
		if (!tasks.containsKey(name)) {
			throw new IllegalArgumentException(String.format("Unknown task: %s.", name));
		}

		return tasks.get(name).getDeclaredConstructor().newInstance();
	}

	public Set<String> listNames() {
		return tasks.keySet();
	}
}
