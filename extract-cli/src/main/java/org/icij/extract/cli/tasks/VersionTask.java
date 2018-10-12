package org.icij.extract.cli.tasks;

import org.icij.task.DefaultTask;

public class VersionTask extends DefaultTask<Void> {

	@Override
	public Void call() throws Exception {
		System.out.println("v2.0.0");
		return null;
	}
}
