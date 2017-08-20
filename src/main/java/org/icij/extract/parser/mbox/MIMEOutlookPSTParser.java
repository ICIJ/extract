package org.icij.extract.parser.mbox;

import com.pff.*;

import org.apache.james.mime4j.dom.*;
import org.apache.james.mime4j.dom.address.Address;
import org.apache.james.mime4j.dom.address.Mailbox;
import org.apache.james.mime4j.dom.field.ParseException;
import org.apache.james.mime4j.message.*;

import org.apache.james.mime4j.stream.RawField;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.extractor.EmbeddedDocumentUtil;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Office;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.microsoft.OutlookExtractor;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static java.lang.String.valueOf;
import static java.util.Collections.singleton;

public class MIMEOutlookPSTParser extends AbstractParser {

	private static final MediaType MS_OUTLOOK_PST_MIME_TYPE = MediaType.application("vnd.ms-outlook-pst");
	private static final Set<MediaType> SUPPORTED_TYPES = singleton(MS_OUTLOOK_PST_MIME_TYPE);
	private static final long serialVersionUID = -6041032996642801405L;

	@Override
	public Set<MediaType> getSupportedTypes(ParseContext context) {
		return SUPPORTED_TYPES;
	}

	@Override
	public void parse(final InputStream in, final ContentHandler handler, final Metadata metadata,
	                  final ParseContext context) throws IOException, SAXException, TikaException {
		final XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
		final TikaInputStream tis = TikaInputStream.get(in);
		final EmbeddedDocumentExtractor embeddedExtractor = EmbeddedDocumentUtil.getEmbeddedDocumentExtractor(context);
		PSTFile pstFile = null;

		metadata.set(Metadata.CONTENT_TYPE, MS_OUTLOOK_PST_MIME_TYPE.toString());
		metadata.set(Metadata.CONTENT_LENGTH, valueOf(tis.getFile().length()));

		xhtml.startDocument();

		try {
			pstFile = new PSTFile(tis.getFile());

			parseFolder(xhtml, pstFile.getRootFolder(), embeddedExtractor);
		} catch (final PSTException | ParseException e) {
			throw new TikaException("A PST parsing exception occurred.", e);
		} finally {
			if (null != pstFile) {
				pstFile.close();
			}
		}

		xhtml.endDocument();
	}

	private void parseFolder(final XHTMLContentHandler xhtml, final PSTFolder pstFolder,
	                         final EmbeddedDocumentExtractor embeddedExtractor)
			throws IOException, PSTException, ParseException, SAXException {
		if (pstFolder.getContentCount() > 0) {
			PSTObject pstObject = pstFolder.getNextChild();

			while (pstObject != null) {
				final AttributesImpl attributes = new AttributesImpl();
				final Metadata objectMetadata = new Metadata();

				// The descriptor node ID is a unique identifier that can be used for quickly retrieving the object from
				// the PST later on: pstObject = PSTObject.detectAndLoadPSTObject(pstFile, id);
				objectMetadata.set("descriptorNodeId", valueOf(pstObject.getDescriptorNodeId()));

				attributes.addAttribute("", "class", "class", "CDATA", "embedded");

				if (pstObject instanceof PSTTask) {
					final PSTTask pstTask = (PSTTask) pstObject;

					//pstTask.
				} else if (pstObject instanceof PSTMessage) {
					final PSTMessage pstMessage = (PSTMessage) pstObject;

					attributes.addAttribute("", "id", "id", "CDATA",
							pstMessage.getInternetMessageId());
					xhtml.startElement("div", attributes);
					xhtml.element("h1", pstMessage.getSubject());

					parseMessage(xhtml, pstMessage, objectMetadata, embeddedExtractor);
				}

				xhtml.endElement("div");

				pstObject = pstFolder.getNextChild();
			}
		}

		if (pstFolder.hasSubfolders()) {
			for (PSTFolder pstSubFolder : pstFolder.getSubFolders()) {
				final AttributesImpl attributes = new AttributesImpl();

				attributes.addAttribute("", "class", "email-folder", "CDATA", "email-folder");

				xhtml.startElement("div", attributes);
				xhtml.element("h1", pstSubFolder.getDisplayName());

				parseFolder(xhtml, pstSubFolder, embeddedExtractor);

				xhtml.endElement("div");
			}
		}
	}

	private void parseMessage(final XHTMLContentHandler xhtml, final PSTMessage pstMessage, final Metadata metadata,
	                          final EmbeddedDocumentExtractor embeddedExtractor)
			throws PSTException, IOException, ParseException, SAXException {
		final Message.Builder builder = Message.Builder.of();
		final Map<Integer, List<Address>> recipientMap = new HashMap<>();

		if (pstMessage.getSenderName() != null) {
			builder.setSender(String.format("\"%s\" <%s>", pstMessage.getSenderName(),
					pstMessage.getSenderEmailAddress()));
		} else {
			builder.setSender(pstMessage.getSenderEmailAddress());
		}

		for (int i = 0, l = pstMessage.getNumberOfRecipients(); i < l; i++) {
			final PSTRecipient pstRecipient = pstMessage.getRecipient(i);
			final List<Address> recipients = recipientMap.computeIfAbsent(pstRecipient.getRecipientType(),
					k -> new ArrayList<>());

			if (pstRecipient.getSmtpAddress() != null) {
				String[] parts = pstRecipient.getSmtpAddress().split("@", 2);

				if (parts.length < 2) {
					parts = new String[2];
					parts[0] = pstRecipient.getSmtpAddress();
					parts[1] = "";
				}

				if (pstRecipient.getDisplayName() != null) {
					recipients.add(new Mailbox(pstRecipient.getDisplayName(), parts[0], parts[2]));
				} else {
					recipients.add(new Mailbox(parts[0], parts[1]));
				}
			} else if (pstRecipient.getDisplayName() != null) {
				recipients.add(new Mailbox(pstRecipient.getDisplayName(), pstRecipient.getEmailAddress(), ""));
			} else {
				recipients.add(new Mailbox(pstRecipient.getEmailAddress(), ""));
			}
		}

		for (Map.Entry<Integer, List<Address>> recipients: recipientMap.entrySet()) {
			switch (recipients.getKey()) {
				case PSTRecipient.MAPI_BCC:
					builder.setBcc(recipients.getValue());
					break;

				case PSTRecipient.MAPI_CC:
					builder.setCc(recipients.getValue());
					break;

				case PSTRecipient.MAPI_TO:
					builder.setTo(recipients.getValue());
					break;
			}
		}

		String importance = null;

		switch (pstMessage.getImportance()) {
			case PSTMessage.IMPORTANCE_HIGH:
				importance = "high";
				break;

			case PSTMessage.IMPORTANCE_NORMAL:
				importance = "normal";
				break;

			case PSTMessage.IMPORTANCE_LOW:
				importance = "low";
				break;
		}

		if (null != importance) {
			builder.setField(new RawField("Importance", importance));
		}

		String priority = null;

		switch (pstMessage.getPriority()) {
			case -1:
				priority = "non-urgent";
				break;

			case 0:
				priority = "normal";
				break;

			case 1:
				priority = "urgent";
				break;
		}

		if (null != priority) {
			builder.setField(new RawField("Priority", priority));
		}

		builder.setMessageId(pstMessage.getInternetMessageId());

		final int attachmentCount = pstMessage.getNumberOfAttachments();

		final String bodyText = pstMessage.getBody(), bodyHtml = pstMessage.getBodyHTML();
		final BodyPart messagePart, textPart, htmlPart;

		if (bodyHtml != null) {
			htmlPart = BodyPartBuilder.create()
					.setBody(bodyHtml, "html", StandardCharsets.UTF_8)
					.build();
		} else {
			htmlPart = null;
		}

		if (bodyText != null) {
			textPart = BodyPartBuilder.create()
					.setBody(bodyText, StandardCharsets.UTF_8)
					.build();
		} else {
			textPart = null;
		}

		if (htmlPart != null && textPart != null) {
			messagePart = BodyPartBuilder.create()
					.setBody(MultipartBuilder.create("alternative")
							.addBodyPart(textPart)
							.addBodyPart(htmlPart)
							.build())
					.build();
		} else if (bodyHtml != null) {
			messagePart = htmlPart;
		} else if (bodyText != null) {
			messagePart = textPart;
		} else {
			messagePart = null;
		}

		if (attachmentCount > 0) {
			final MultipartBuilder bodyBuilder = MultipartBuilder.create("mixed");

			// Add the main message.
			if (messagePart != null) {
				bodyBuilder.addBodyPart(messagePart);
			}

			// Add attachments.
			for (int i = 0; i < attachmentCount; i++) {
				final PSTAttachment attachment = pstMessage.getAttachment(i);
				final byte[] buffer = new byte[attachment.getFilesize()];

				try (final DataInputStream dis = new DataInputStream(attachment.getFileInputStream())) {
					dis.readFully(buffer);
				}

				bodyBuilder.addBinaryPart(buffer, attachment.getMimeTag());
			}

			builder.setBody(bodyBuilder.build());
		} else if (messagePart != null) {
			builder.setBody(messagePart.getBody());
		}

		metadata.set(Metadata.CONTENT_TYPE, "message/rfc822");
		metadata.set(Metadata.RESOURCE_NAME_KEY, pstMessage.getInternetMessageId());
		metadata.set(Metadata.EMBEDDED_RELATIONSHIP_ID, pstMessage.getInternetMessageId());
		metadata.set(TikaCoreProperties.IDENTIFIER, pstMessage.getInternetMessageId());
		metadata.set(Metadata.MESSAGE_FROM, pstMessage.getSenderName());
		metadata.set(TikaCoreProperties.CREATOR, pstMessage.getSenderName());
		metadata.set(TikaCoreProperties.CREATED, pstMessage.getCreationTime());
		metadata.set(TikaCoreProperties.MODIFIED, pstMessage.getLastModificationTime());
		metadata.set(TikaCoreProperties.COMMENTS, pstMessage.getComment());

		metadata.set("senderEmailAddress", pstMessage.getSenderEmailAddress());
		metadata.set("recipients", pstMessage.getRecipientsString());
		metadata.set("displayTo", pstMessage.getDisplayTo());
		metadata.set("displayCC", pstMessage.getDisplayCC());
		metadata.set("displayBCC", pstMessage.getDisplayBCC());
		metadata.set("importance", valueOf(pstMessage.getImportance()));
		metadata.set("priority", valueOf(pstMessage.getPriority()));
		metadata.set("flagged", valueOf(pstMessage.isFlagged()));
		metadata.set(Office.MAPI_MESSAGE_CLASS,
				OutlookExtractor.getMessageClass(pstMessage.getMessageClass()));

		metadata.set(org.apache.tika.metadata.Message.MESSAGE_FROM_EMAIL, pstMessage.getSenderEmailAddress());

		metadata.set(Office.MAPI_FROM_REPRESENTING_EMAIL,
				pstMessage.getSentRepresentingEmailAddress());

		metadata.set(org.apache.tika.metadata.Message.MESSAGE_FROM_NAME, pstMessage.getSenderName());
		metadata.set(Office.MAPI_FROM_REPRESENTING_NAME, pstMessage.getSentRepresentingName());

		final Message message = builder.build();

		try (final ByteArrayOutputStream boas = new ByteArrayOutputStream()) {
			new DefaultMessageWriter().writeMessage(message, boas);

			embeddedExtractor.parseEmbedded(new ByteArrayInputStream(boas.toByteArray()), xhtml, metadata, false);
		} finally {
			message.dispose();
		}
	}
}
