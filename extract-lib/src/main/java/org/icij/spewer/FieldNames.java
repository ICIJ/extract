package org.icij.spewer;

import org.icij.task.Options;
import org.icij.task.annotation.Option;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Defaults for use with spewers.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Option(name = "idField", description = "Index field for an automatically generated identifier. The ID " +
		"for the same file is guaranteed not to change if the path doesn't change. Defaults to \"id\".", code = "i",
		parameter = "name")
@Option(name = "textField", description = "Field name for extracted text.", code = "t", parameter = "name")
@Option(name = "pathField", description = "Field name for the file path.", parameter = "name", code = "p")
@Option(name = "parentPathField", description = "Field name for the parent directory path.", parameter = "name")
@Option(name = "parentField", description = "Field name for the parent ID on child documents.", parameter = "name")
@Option(name = "levelField", description = "Field name for the hierarchy level field.", parameter = "name")
@Option(name = "baseTypeField", description = "Field name for the base content-type.", parameter = "name")
@Option(name = "versionField", description = "Index field name for the version.", parameter = "name")
@Option(name = "tagPrefix", description = "Prefix for tag fields added to the index.", parameter = "name")
@Option(name = "metadataPrefix", description = "Prefix for metadata fields added to the index.", parameter = "name")
@Option(name = "metadataISODatePostfix", description = "Postfix for 'fixed' ISO 8601 metadata fields.", parameter =
		"name")
public class FieldNames {

	private static final Pattern fieldName = Pattern.compile("[^A-Za-z0-9_]");

	public static final String DEFAULT_ID_FIELD = "extract_id";
	public static final String DEFAULT_TEXT_FIELD = "tika_content";
	public static final String DEFAULT_PATH_FIELD = "extract_paths";
	public static final String DEFAULT_BASE_TYPE_FIELD = "extract_base_type";
	public static final String DEFAULT_PARENT_PATH_FIELD = "extract_parent_paths";
	public static final String DEFAULT_PARENT_ID_FIELD = "extract_parent_id";
	public static final String DEFAULT_ROOT_FIELD = "extract_root";
	public static final String DEFAULT_LEVEL_FIELD = "extract_level";
	public static final String DEFAULT_VERSION_FIELD = "_version_";
	public static final String DEFAULT_METADATA_FIELD_PREFIX = "tika_metadata_";
	public static final String DEFAULT_TAG_FIELD_PREFIX = "tag_";
	public static final String DEFAULT_METADATA_ISO_DATE_POSTFIX = "_iso8601";

	private String textField = DEFAULT_TEXT_FIELD;
	private String pathField = DEFAULT_PATH_FIELD;
	private String parentPathField = DEFAULT_PARENT_PATH_FIELD;
	private String idField = DEFAULT_ID_FIELD;
	private String baseTypeField = DEFAULT_BASE_TYPE_FIELD;
	private String versionField = DEFAULT_VERSION_FIELD;
	private String parentIdField = DEFAULT_PARENT_ID_FIELD;
	private String rootField = DEFAULT_ROOT_FIELD;
	private String levelField = DEFAULT_LEVEL_FIELD;
	private String tagFieldPrefix = DEFAULT_TAG_FIELD_PREFIX;
	private String metadataFieldPrefix = DEFAULT_METADATA_FIELD_PREFIX;
	private String metadataISODatePostfix = DEFAULT_METADATA_ISO_DATE_POSTFIX;

	public FieldNames configure(final Options<String> options) {
		options.get("textField").value().ifPresent(this::forText);
		options.get("pathField").value().ifPresent(this::forPath);
		options.get("parentPathField").value().ifPresent(this::forParentPath);
		options.get("parentField").value().ifPresent(this::forParentId);
		options.get("levelField").value().ifPresent(this::forLevel);
		options.get("baseTypeField").value().ifPresent(this::forBaseType);
		options.get("versionField").value().ifPresent(this::forVersion);
		options.get("idField").value().ifPresent(this::forId);
		options.get("tagPrefix").value().ifPresent(this::forTagPrefix);
		options.get("metadataPrefix").value().ifPresent(this::forMetadataPrefix);
		options.get("metadataISODatePostfix").value().ifPresent(this::forMetadataISODatePostfix);

		return this;
	}

	private void forText(final String textField) {
		this.textField = textField;
	}

	public String forText() {
		return textField;
	}

	private void forPath(final String pathField) {
		this.pathField = pathField;
	}

	public String forPath() {
		return pathField;
	}

	private void forParentPath(final String parentPathField) {
		this.parentPathField = parentPathField;
	}

	public String forParentPath() {
		return parentPathField;
	}

	private void forParentId(final String parentIdField) {
		this.parentIdField = parentIdField;
	}

	public String forParentId() {
		return parentIdField;
	}

	private void forRoot(final String rootField) {
		this.rootField = rootField;
	}

	public String forRoot() {
		return rootField;
	}

	private void forLevel(final String levelField) {
		this.levelField = levelField;
	}

	public String forLevel() {
		return levelField;
	}

	private void forBaseType(final String baseTypeField) {
		this.baseTypeField = baseTypeField;
	}

	public String forBaseType() {
		return baseTypeField;
	}

	private void forVersion(final String versionField) {
		this.versionField = versionField;
	}

	public String forVersion() {
		return versionField;
	}

	private void forId(final String idField) {
		this.idField = idField;
	}

	public String forId() {
		return idField;
	}

	private void forMetadataPrefix(final String metadataFieldPrefix) {
		this.metadataFieldPrefix = metadataFieldPrefix;
	}

	public String forMetadata(final String name) {
		final String normalizedName = fieldName.matcher(name).replaceAll("_").toLowerCase(Locale.ROOT);

		if (null != metadataFieldPrefix) {
			return metadataFieldPrefix + normalizedName;
		}

		return normalizedName;
	}

	private void forMetadataISODatePostfix(final String metadataISODatePostfix) {
		this.metadataISODatePostfix = metadataISODatePostfix;
	}

	public String forMetadataISODate(final String name) {
		return forMetadata(name) + metadataISODatePostfix;
	}

	private void forTagPrefix(final String tagFieldPrefix) {
		this.tagFieldPrefix = tagFieldPrefix;
	}

	public String forTag(final String name) {
		return tagFieldPrefix + name;
	}
}
