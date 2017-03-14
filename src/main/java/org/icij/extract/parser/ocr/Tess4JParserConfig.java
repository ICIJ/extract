package org.icij.extract.parser.ocr;

import net.sourceforge.tess4j.ITessAPI;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Configuration options and defaults for the Tess4J parser.
 *
 * On instantiation, the constructor will search for training data in system-default paths.
 */
public class Tess4JParserConfig implements Serializable {

	private static final long serialVersionUID = -3693268974548732L;

	private String dataPath;
	private String language = "eng";
	private int pageSegMode = ITessAPI.TessPageSegMode.PSM_AUTO_OSD;
	private int ocrEngineMode = ITessAPI.TessOcrEngineMode.OEM_DEFAULT;
	private int minFileSizeToOcr = 0;
	private int maxFileSizeToOcr = Integer.MAX_VALUE;
	private int timeout = 120;

	/**
	 * Instantiate a new configuration object.
	 *
	 * This triggers a search of local paths for Tesseract training data.
	 */
	public Tess4JParserConfig() {
		Stream.of("/usr/share/tesseract-ocr/tessdata", "/usr/local/share/tessdata")
				.map(Paths::get)
				.filter(Files::isDirectory)
				.findAny().ifPresent(path -> setDataPath(path.getParent().toAbsolutePath().toString() + "/"));
	}

	/**
	 * Get the path to the Tesseract training data.
	 *
	 * @return the path to the training data files
	 */
	public String getDataPath() {
		return dataPath;
	}

	/**
	 * Set the path to the Tesseract data or 'tessdata' directory, which contains training data files.
	 *
	 * In some cases (such as on Windows), this folder is found in the Tesseract installation, but in other cases
	 * (such as when Tesseract is built from source), it may be located elsewhere.
	 *
	 * @param dataPath the path to the training data files
	 */
	public void setDataPath(final String dataPath) {
		if (!dataPath.isEmpty() && !dataPath.endsWith(File.separator)) {
			this.dataPath = dataPath + File.separator;
		} else {
			this.dataPath = dataPath;
		}
	}

	/**
	 * Get the Tesseract language training data to be used.
	 *
	 * @return the language codes, separated by plus characters
	 */
	public String getLanguage() {
		return language;
	}

	/**
	 * Set the Tesseract language training data to be used. The default is "eng".
	 *
	 * Multiple languages may be specified, separated by plus characters.
	 *
	 * @param language the language to set
	 */
	public void setLanguage(final String language) {
		this.language = language;
	}

	/**
	 * Get the Tesseract page segmentation mode.
	 *
	 * @return the page segmentation mode
	 */
	public int getPageSegMode() {
		return pageSegMode;
	}

	/**
	 * Set the Tesseract page segmentation mode.
	 *
	 * The default is 1: automatic page segmentation with OSD (Orientation and Script Detection).
	 *
	 * @param pageSegMode the page segmentation mode
	 */
	public void setPageSegMode(final int pageSegMode) {
		this.pageSegMode = pageSegMode;
	}

	/**
	 * Get the minimum size of a file that will be submitted to OCR.
	 *
	 * @return the minimum file size in bytes
	 */
	public int getMinFileSizeToOcr() {
		return minFileSizeToOcr;
	}

	/**
	 * Set the minimum file size to submit to OCR.
	 *
	 * The default is 0.
	 *
	 * @param minFileSizeToOcr the minimum file size in bytes
	 */
	public void setMinFileSizeToOcr(final int minFileSizeToOcr) {
		this.minFileSizeToOcr = minFileSizeToOcr;
	}

	/**
	 * Get the maximum size of a file that will be submitted to OCR.
	 *
	 * @return the maximum file size in bytes
	 */
	public int getMaxFileSizeToOcr() {
		return maxFileSizeToOcr;
	}

	/**
	 * Set maximum file size to submit to OCR.
	 *
	 * The default is Integer.MAX_VALUE.
	 *
	 * @param maxFileSizeToOcr the maximum file size in bytes
	 */
	public void setMaxFileSizeToOcr(final int maxFileSizeToOcr) {
		this.maxFileSizeToOcr = maxFileSizeToOcr;
	}

	/**
	 * Set maximum time (seconds) to wait for the OCR process to terminate.
	 *
	 * The default value is 120s.
	 *
	 * @param timeout timeout in seconds
	 */
	public void setTimeout(final int timeout) {
		this.timeout = timeout;
	}

	/**
	 * Get the timeout in seconds before Tesseract will be forced to stop OCR.
	 *
	 * @return timeout in seconds
	 */
	public int getTimeout() {
		return timeout;
	}

	/**
	 * Get the OCR engine mode.
	 *
	 * @return the OCR engine mode
	 */
	public int getOcrEngineMode() {
		return ocrEngineMode;
	}

	/**
	 * Set the OCR engine mode.
	 *
	 * @param ocrEngineMode the OCR engine mode
	 */
	public void setOcrEngineMode(final int ocrEngineMode) {
		this.ocrEngineMode = ocrEngineMode;
	}
}
