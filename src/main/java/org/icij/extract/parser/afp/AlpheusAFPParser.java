package org.icij.extract.parser.afp;

import com.mgz.afp.base.StructuredField;
import com.mgz.afp.base.StructuredFieldIntroducer;
import com.mgz.afp.enums.SFTypeID;
import com.mgz.afp.exceptions.AFPParserException;
import com.mgz.afp.parser.AFPParser;
import com.mgz.afp.parser.AFPParserConfiguration;
import com.mgz.afp.ptoca.PTX_PresentationTextData;
import com.mgz.afp.ptoca.controlSequence.PTOCAControlSequence;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import static java.util.Collections.singleton;

/**
 * Parser for Advanced Function Presentation (AFP) files.
 */
public class AlpheusAFPParser extends AbstractParser {

	private static final MediaType AFP_MIMETYPE = MediaType.application("vnd.ibm.modcap");

	private static final Set<MediaType> SUPPORTED_TYPES = singleton(AFP_MIMETYPE);
	private static final long serialVersionUID = 24852186799246269L;

	@Override
	public Set<MediaType> getSupportedTypes(final ParseContext context) {
		return SUPPORTED_TYPES;
	}

	@Override
	public void parse(final InputStream stream, final ContentHandler handler, final Metadata metadata,
	                  final ParseContext context) throws IOException, SAXException, TikaException {

		final XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
		final AFPParserConfiguration pc = new AFPParserConfiguration();

		pc.setInputStream(new CloseShieldInputStream(stream));

		final AFPParser parser = new AFPParser(pc);
		StructuredField sf;

		xhtml.startDocument();

		try {
			while (null != (sf = parser.parseNextSF())) {
				System.err.println("iterate");
				final StructuredFieldIntroducer sfi = sf.getStructuredFieldIntroducer();

				if (sfi.getSFTypeID().equals(SFTypeID.PTX_PresentationTextData)) {
					final PTX_PresentationTextData ptx = new PTX_PresentationTextData();

					ptx.decodeAFP(sfi.getExtensionData(), 0, sfi.getExtensionLength(), pc);
					for (PTOCAControlSequence pcs: ptx.getControlSequences()) {
						final PTOCAControlSequence.ControlSequenceIntroducer pci = pcs.getCsi();

						if (pci.getControlSequenceFunctionType().equals(PTOCAControlSequence
								.ControlSequenceFunctionType.TRN_TransparentData)) {
							final PTOCAControlSequence.TRN_TransparentData trn = new PTOCAControlSequence
									.TRN_TransparentData();

							trn.decodeAFP(pci.toBytes(), 0, pci.getLength(), pc);

							final String s = trn.getTransparentData();

							if (null != s && !s.isEmpty()) {
								xhtml.startElement("p");
								xhtml.characters(s);
								xhtml.endElement("p");
							}
						}
					}
				}
			}
		} catch (final AFPParserException e) {
			throw new TikaException(e.getMessage(), e);
		} finally {
			xhtml.endDocument();
		}
	}
}
