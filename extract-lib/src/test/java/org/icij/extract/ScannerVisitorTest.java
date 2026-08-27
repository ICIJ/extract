package org.icij.extract;

import org.icij.extract.queue.DocumentQueue;
import org.icij.extract.queue.MemoryDocumentQueue;
import org.icij.task.Options;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;
import org.mockito.*;
import org.mockito.MockitoAnnotations;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class ScannerVisitorTest {
  private static final String LOSSY = "will not survive the queue";

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  DocumentQueue<Path> smallQueue = new MemoryDocumentQueue<>("extract:small:queue", 2);

  final Path root = Paths.get(getClass().getResource("/documents/text/").toURI());
  Options<String> options = Options.from(new HashMap<>() {{
    put("queueFullTimeout", 1);
  }});
  @Spy
  private ScannerVisitor scannerVisitor =new ScannerVisitor(root,smallQueue,options);

    public ScannerVisitorTest() throws URISyntaxException {
    }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void test_scanner_visitor() throws Throwable {

    BasicFileAttributes attr = Files.readAttributes(root, BasicFileAttributes.class);
    doNothing().when(scannerVisitor).onQueueFull(root, attr);
    scannerVisitor.queue(root,attr);
    scannerVisitor.queue(root,attr);
    scannerVisitor.queue(root,attr);
    verify(scannerVisitor).onQueueFull(root,attr);
  }

  @Test
  public void test_lossy_file_name_is_warned_once_and_counted() throws Throwable {
    createInTmp("printf x > \"$(printf 'bad\\377name.txt')\"");
    Path scanned = onlyEntryOfTmp();
    assumeTrue("file name decoded without loss, nothing to test", scanned.toString().contains("\ufffd"));

    List<ILoggingEvent> events = scanCapturingLogs();

    assertThat(messagesContaining(events, Level.WARN, LOSSY)).hasSize(1);
    assertThat(messagesContaining(events, Level.WARN, LOSSY).get(0)).contains(scanned.toUri().toString());
    assertThat(messagesContaining(events, Level.INFO, "Completed scan of")).hasSize(1);
    assertThat(messagesContaining(events, Level.INFO, "Completed scan of").get(0))
            .contains("1 name(s) " + LOSSY);
  }

  @Test
  public void test_lossy_directory_is_warned_once_for_the_whole_subtree() throws Throwable {
    createInTmp("d=\"$(printf 'bad\\377dir')\"; mkdir \"$d\" && printf x > \"$d/a.txt\" && printf y > \"$d/b.txt\"");
    Path scanned = onlyEntryOfTmp();
    assumeTrue("directory name decoded without loss, nothing to test", scanned.toString().contains("\ufffd"));

    List<ILoggingEvent> events = scanCapturingLogs();

    assertThat(messagesContaining(events, Level.WARN, LOSSY)).hasSize(1);
    assertThat(messagesContaining(events, Level.WARN, LOSSY).get(0)).contains(scanned.toUri().toString());
    assertThat(messagesContaining(events, Level.INFO, "Completed scan of").get(0))
            .contains("3 name(s) " + LOSSY);
  }

  @Test
  public void test_names_that_survive_the_queue_are_not_warned_nor_counted() throws Throwable {
    tmp.newFile("plain.txt");

    List<ILoggingEvent> events = scanCapturingLogs();

    assertThat(messagesContaining(events, Level.WARN, LOSSY)).isEmpty();
    assertThat(messagesContaining(events, Level.INFO, "Completed scan of").get(0)).excludes(LOSSY);
  }

  private void createInTmp(final String script) throws Exception {
    Process process = new ProcessBuilder("sh", "-c", script).directory(tmp.getRoot()).start();
    assumeTrue("file system refused a name that is not valid UTF-8", process.waitFor() == 0);
  }

  private Path onlyEntryOfTmp() throws Exception {
    try (Stream<Path> entries = Files.list(tmp.getRoot().toPath())) {
      return entries.findFirst().orElseThrow();
    }
  }

  private List<ILoggingEvent> scanCapturingLogs() throws Exception {
    Logger log = (Logger) org.slf4j.LoggerFactory.getLogger(ScannerVisitor.class);
    Level originalLevel = log.getLevel();
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    log.addAppender(appender);
    log.setLevel(Level.INFO);

    try {
      new ScannerVisitor(tmp.getRoot().toPath(), new MemoryDocumentQueue<>("extract:lossy:queue", 100), options).call();
      return List.copyOf(appender.list);
    } finally {
      log.detachAppender(appender);
      log.setLevel(originalLevel);
    }
  }

  private List<String> messagesContaining(final List<ILoggingEvent> events, final Level level, final String needle) {
    return events.stream()
            .filter(e -> e.getLevel() == level)
            .map(ILoggingEvent::getFormattedMessage)
            .filter(m -> m.contains(needle))
            .toList();
  }

  @Test(expected = InterruptedException.class)
  public void test_scanner_visitor_with_exception() throws Throwable {
    DocumentQueue<Path> smallQueue = new MemoryDocumentQueue<>("extract:small:queue", 2);
    final Path root = Paths.get(getClass().getResource("/documents/text/").toURI());
    Options<String> options = Options.from(new HashMap<>() {{
      put("queueFullTimeout", 1);
      put("queueFullStop", true);
    }});
    ScannerVisitor scannerVisitor = new ScannerVisitor(root, smallQueue, options);
    BasicFileAttributes attr = Files.readAttributes(root, BasicFileAttributes.class);

    scannerVisitor.queue(root,attr);
    scannerVisitor.queue(root,attr);
    scannerVisitor.queue(root,attr);
  }
}