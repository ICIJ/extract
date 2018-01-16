package org.icij.extract.tasks;

import org.icij.extract.queue.DocumentQueue;
import org.icij.extract.queue.DocumentQueueFactory;
import org.icij.task.DefaultTask;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;
import org.icij.task.annotation.Task;

/**
 * CLI class for wiping a {@link DocumentQueue} from the backend.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Task("Wipe a queue. The name option is respected.")
@OptionsClass(DocumentQueueFactory.class)
public class WipeQueueTask extends DefaultTask<Integer> {

	@Override
	public Integer run() throws Exception {
		final int cleared;

		try (final DocumentQueue queue = new DocumentQueueFactory(options).createShared()) {
			cleared = queue.size();
			queue.clear();
		} catch (Exception e) {
			throw new RuntimeException("Exception while wiping queue.", e);
		}

		return cleared;
	}
}
