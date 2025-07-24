package org.icij.extract;

import org.icij.extract.queue.DocumentQueue;
import org.icij.extract.queue.MemoryDocumentQueue;
import org.icij.task.Options;
import org.junit.Before;
import org.junit.Test;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import org.mockito.*;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class ScannerVisitorTest {
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