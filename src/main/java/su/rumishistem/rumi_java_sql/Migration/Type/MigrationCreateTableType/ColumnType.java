package su.rumishistem.rumi_java_sql.Migration.Type.MigrationCreateTableType;

public class ColumnType {
	public final Name name;
	public final int size;

	public ColumnType(Name name) {
		this.name = name;
		this.size = -1;
	}


	public ColumnType(Name name, int size) {
		this.name = name;
		this.size = size;
	}

	public enum Name {
		Varchar,
		Text,
		Int,
		Date,
		DateTime
	}
}