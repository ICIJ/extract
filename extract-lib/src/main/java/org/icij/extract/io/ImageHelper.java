package org.icij.extract.io;

import static net.sourceforge.tess4j.util.ImageIOHelper.getImageFileFormat;
import static org.icij.extract.LambdaExceptionUtils.rethrowFunction;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

public class ImageHelper {

    public static Stream<BufferedImage> getTiffImages(InputStream inputStream) throws IOException {
        String fileFormat = getImageFileFormat(new File("placeholder.tiff" ));
        Iterator<ImageReader> iterators = ImageIO.getImageReadersByFormatName(fileFormat);
        if (!iterators.hasNext()) {
            throw new RuntimeException("May need to install JAI Image I/O package.\nhttps://github.com/jai-imageio/jai-imageio-core");
        } else {
            ImageReader reader = iterators.next();
            try {
                try (ImageInputStream imageInputStream = ImageIO.createImageInputStream(inputStream)) {
                    reader.setInput(imageInputStream);
                    return IntStream.range(0, reader.getNumImages(true))
                        .boxed().map(rethrowFunction(reader::read));
                }
            } finally {
                if (reader != null) {
                    reader.dispose();
                }
            }
        }
    }

}
