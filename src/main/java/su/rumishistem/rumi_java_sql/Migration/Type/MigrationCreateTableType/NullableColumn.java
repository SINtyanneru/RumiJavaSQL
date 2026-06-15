package su.rumishistem.rumi_java_sql.Migration.Type.MigrationCreateTableType;

public class NullableColumn extends Column {
	public NullableColumn(String name, String description, ColumnType type, ColumnConstraints constraints) {
		super(name, description, type, constraints);
	}
}
