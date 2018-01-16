package org.icij.task;

import org.icij.kaxxa.events.Listener;
import org.icij.kaxxa.events.Monitor;
import org.icij.kaxxa.events.Monitorable;
import org.icij.kaxxa.events.DefaultMonitor;

public abstract class MonitorableTask<R> extends DefaultTask<R> implements Monitorable {

	protected final Monitor monitor;

	protected MonitorableTask() {
		this.monitor = new DefaultMonitor();
	}

	protected MonitorableTask(final Monitor monitor) {
		this.monitor = monitor;
	}

	@Override
	public void addListener(final Listener listener) {
		monitor.addListener(listener);
	}

	@Override
	public void deleteListener(final Listener listener) {
		monitor.deleteListener(listener);
	}

	@Override
	public void deleteListeners() {
		monitor.deleteListeners();
	}

	@Override
	public int countListeners() {
		return monitor.countListeners();
	}
}
