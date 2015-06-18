package org.icij.extract.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.concurrent.Executor;

import org.apache.tika.parser.Parser;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.sax.BodyContentHandler;

import org.xml.sax.ContentHandler;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;

/**
* Reader for the text content from a given binary stream. This class
* uses a background parsing task with a {@link Parser} to parse the text 
* content from a given input stream. The {@link BodyContentHandler} class
* and a pipe is used to convert the push-based SAX event stream to the
* pull-based character stream defined by the {@link Reader} interface.
*
* Based on an implementation from the Tika source. This version adds
* functionality for markup output.
*
* @since 1.0.0-beta
*/
public class TextParsingReader extends ParsingReader {

	/**
	 * Creates a reader for the text content of the given binary stream.
	 *
	 * @param stream binary stream
	 * @throws IOException if the document can not be parsed
	 */
	public TextParsingReader(InputStream stream) throws IOException {
		super(stream);
	}
	
	/**
	 * Creates a reader for the text content of the given binary stream
	 * with the given name.
	 *
	 * @param stream binary stream
	 * @param name document name
	 * @throws IOException if the document can not be parsed
	 */
	public TextParsingReader(InputStream stream, String name) throws IOException {
		super(stream, name);
	}

	/**
	 * Creates a reader for the text content of the given file.
	 *
	 * @param file file
	 * @throws FileNotFoundException if the given file does not exist
	 * @throws IOException if the document can not be parsed
	 */
	public TextParsingReader(File file) throws FileNotFoundException, IOException {
		super(file);
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
	 * @param stream binary stream
	 * @param metadata document metadata
	 * @param context parsing context
	 * @throws IOException if the document can not be parsed
	 */
	public TextParsingReader(Parser parser, InputStream stream, Metadata metadata, ParseContext context) throws IOException {
		super(parser, stream, metadata, context);
	}

	@Override
	protected void execute() {
		executor.execute(new ParsingTask());
	}

	/**
	 * The background parsing task.
	 */
	protected class ParsingTask extends ParsingReader.ParsingTask {

	    /**
	     * Parses the given binary stream and writes the text content
	     * to the write end of the pipe. Potential exceptions (including
	     * the one caused if the read end is closed unexpectedly) are
	     * stored before the input stream is closed and processing is stopped.
	     */
		public void run() {
			handler = new BodyContentHandler(writer);
			super.run();
		}
	}
}
