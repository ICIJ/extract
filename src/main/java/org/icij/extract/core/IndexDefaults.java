package org.icij.extract.core;

/**
 * Defaults for use with Solr.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class IndexDefaults {

	public static final String DEFAULT_ID_FIELD = "id";
	public static final String DEFAULT_TEXT_FIELD = "content";
	public static final String DEFAULT_PATH_FIELD = "path";
	public static final String DEFAULT_BASE_TYPE_FIELD = "metadata_base_type";
	public static final String DEFAULT_PARENT_PATH_FIELD = "metadata_parent_path";
	public static final String DEFAULT_METADATA_FIELD_PREFIX = "metadata_";

	/**
	 * The default algorithm used for calculating ID hashes.
	 *
	 * The standard names are defined in the <a href="http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#MessageDigest">Oracle Standard Algorithm Name
	 * Documentation</a>.
	 */
	public static final String DEFAULT_ID_ALGORITHM = "SHA-256";
}
