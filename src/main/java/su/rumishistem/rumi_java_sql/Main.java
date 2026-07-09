package su.rumishistem.rumi_java_sql;

import java.sql.SQLException;

import su.rumishistem.rumi_java_sql.Migration.SQLMigrationer;
import su.rumishistem.rumi_java_sql.Migration.Type.MigrationCreateTableType.*;

public class Main {
	public static void main(String[] args) throws SQLException {
		SQL.connect("192.168.0.130", "3306", "BetaRumiServer", "rumiserver_beta", "1234");

		SQLMigrationer.add(
			new MigrationCreateTable("TEST", new Column[]{
				new Column("ID", "主キー", new ColumnType(ColumnType.Name.Varchar, 256), ColumnConstraints.PrimaryKey),
				new Column("A", "あ？", new ColumnType(ColumnType.Name.Varchar, 256), ColumnConstraints.None),
				new Column("I", "い？", new ColumnType(ColumnType.Name.Varchar, 256), ColumnConstraints.None),
				new Column("CONTENTS", "内容物", new ColumnType(ColumnType.Name.Text), ColumnConstraints.None)
			},new ReferenceColumn[0], new UniqueColumn[] {
				new UniqueColumn("unq_ai", new String[]{"A", "I"})
			})
		);

		SQLMigrationer.migration("SQL_TEST");
	}
}
