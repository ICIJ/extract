package org.icij.extract.parser;

import java.io.IOException;
import java.io.InputStream;

import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.sax.BodyContentHandler;

/**
 * Reader for the text content from a given binary stream. This class
 * uses a background parsing task with a {@link Parser} to parse the text
 * content from a given input stream. The {@link BodyContentHandler} class
 * and a pipe is used to convert the push-based SAX event stream to the
 * pull-based character stream defined by the {@link java.io.Reader} interface.
 *
 * @since 1.0.0-beta
 */
public class TextParsingReader extends ParsingReader {

	/**
	 * Creates a reader for the content of the given binary stream.
	 *
	 * @param input binary stream
	 * @throws IOException if the document can not be parsed
	 */
	public TextParsingReader(final InputStream input) throws IOException {
		super(input);
	}
	
	/**
	 * Creates a reader for the content of the given binary stream with the given name.
	 *
	 * @param input binary stream
	 * @param name document name
	 * @throws IOException if the document can not be parsed
	 */
	public TextParsingReader(final InputStream input, final String name) throws IOException {
		super(input, name);
	}

	/**
	 * Creates a reader for the text content of the given binary stream
	 * with the given document metadata. The given parser is used for the
	 * parsing task that is run with the given executor.
	 *
	 * The created reader will be responsible for closing the given stream.
	 * The stream and any associated resources will be closed at or before
	 * the time when the {@link #close()} method is called on this reader.
	 *
	 * @param parser parser instance
	 * @param input binary stream
	 * @param metadata document metadata
	 * @param context parsing context
	 * @throws IOException if the document can not be parsed
	 */
	public TextParsingReader(final Parser parser, final InputStream input, final Metadata metadata, final ParseContext
			context) throws
			IOException {
		super(parser, input, metadata, context);
	}

	@Override
	protected void execute() {
		executor.execute(new ParsingTask());
	}

	/**
	 * The background parsing task.
	 */
	private class ParsingTask extends ParsingReader.ParsingTask {

	    /**
	     * Parses the given binary stream and writes the text content
	     * to the write end of the pipe. Potential exceptions (including
	     * the one caused if the read end is closed unexpectedly) are
	     * stored before the input stream is closed and processing is stopped.
	     */
		@Override
		public void run() {
			handler = new BodyContentHandler(writer);
			super.run();
		}
	}
}
