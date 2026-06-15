package su.rumishistem.rumi_java_sql.Migration.Type.MigrationCreateTableType;

public class ReferenceColumn {
	public final String foreign;
	public final String reference_table;
	public final String reference_column;

	public ReferenceColumn(String foreign, String reference_table, String reference_column) {
		this.foreign = foreign;
		this.reference_table = reference_table;
		this.reference_column = reference_column;
	}
}
