package su.rumishistem.rumi_java_sql.Migration.Type.MigrationCreateTableType;

public class Column {
	public final String name;
	public final String description;
	public final ColumnType type;
	public final ColumnConstraints constraints;

	public Column(String name, String description, ColumnType type, ColumnConstraints constraints) {
		this.name = name;
		this.description = description;
		this.type = type;
		this.constraints = constraints;
	}
}
