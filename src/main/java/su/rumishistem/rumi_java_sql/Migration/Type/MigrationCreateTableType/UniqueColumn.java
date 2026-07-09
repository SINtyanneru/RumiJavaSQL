package su.rumishistem.rumi_java_sql.Migration.Type.MigrationCreateTableType;

public class UniqueColumn {
	public final String name;
	public final String[] column_list;

	public UniqueColumn(String name, String[] column_list) {
		this.name = name;
		this.column_list = column_list;
	}
}
