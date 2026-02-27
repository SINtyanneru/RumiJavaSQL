package su.rumishistem.rumi_java_sql;

import java.sql.Blob;
import java.sql.SQLException;

public class SQLValue {
	private final Object value;

	public SQLValue(Object value) {
		this.value = value;
	}

	public boolean is_null() {
		return value == null;
	}

	public Object as_object() {
		return value;
	}

	public String as_string() {
		if (value instanceof String) {
			return (String) value;
		} else {
			return value.toString();
		}
	}

	public int as_int() {
		return (int) value;
	}

	public long as_long() {
		return (long) value;
	}

	public Blob as_blob() {
		return (Blob) value;
	}

	public byte[] as_byte() throws SQLException{
		Blob blob = as_blob();
		byte[] data = blob.getBytes(1, (int)blob.length());
		return data;
	}

	public boolean as_boolean() {
		return (boolean) value;
	}
}
