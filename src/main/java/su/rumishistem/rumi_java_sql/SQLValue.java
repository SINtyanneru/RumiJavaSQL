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
		if (this.value instanceof byte[]) {
			return (byte[])this.value;
		} else if (this.value instanceof Blob) {
			Blob blob = as_blob();
			byte[] data = blob.getBytes(1, (int)blob.length());
			return data;
		} else {
			throw new ClassCastException("なぜかbyte[]でもBlobでもない「"+this.value.getClass().getName()+"」が来た");
		}
	}

	public boolean as_boolean() {
		return (boolean) value;
	}
}
