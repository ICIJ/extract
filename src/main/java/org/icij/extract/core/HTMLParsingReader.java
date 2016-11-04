/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.icij.extract.core;

import java.io.IOException;
import java.io.InputStream;

import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.sax.ExpandedTitleContentHandler;

import org.xml.sax.ContentHandler;

/**
* Reader for the text content from a given binary stream.
*
* @since 1.0.0-beta
*/
public class HTMLParsingReader extends ParsingReader {

	/**
	 * Creates a reader for the HTML content of the given binary stream with the given document metadata. The given
	 * parser is used for the parsing task that is run with the given executor.
	 *
	 * The created reader will be responsible for closing the given stream.
	 *
	 * The stream and any associated resources will be closed at or before the time when the {@link #close()} method
	 * is called on this reader.
	 *
	 * @param parser parser instance
	 * @param input binary stream
	 * @param metadata document metadata
	 * @param context parsing context
	 * @throws IOException if the document can not be parsed
	 */
	public HTMLParsingReader(Parser parser, InputStream input, Metadata metadata, ParseContext context)
		throws IOException {
		super(parser, input, metadata, context);
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
		 * Constructs the transformer handler that will be used to transform
		 * the SAX events emitted by the parser to HTML 5.
		 */
		protected ContentHandler createHandler() {
			return new ExpandedTitleContentHandler(new HTMLSerializer(writer));
		}

	    /**
	     * Parses the given binary stream and writes the HTML content
	     * to the write end of the pipe. Potential exceptions (including
	     * the one caused if the read end is closed unexpectedly) are
	     * stored before the input stream is closed and processing is stopped.
	     */
		@Override
		public void run() {
			handler = createHandler();
			super.run();
		}
	}
}
