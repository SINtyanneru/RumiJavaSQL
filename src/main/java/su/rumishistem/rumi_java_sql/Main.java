package su.rumishistem.rumi_java_sql;

import java.sql.SQLException;

import su.rumishistem.rumi_java_sql.Migration.SQLMigrationer;
import su.rumishistem.rumi_java_sql.Migration.Type.MigrationCreateTable;

public class Main {
	public static void main(String[] args) throws SQLException {
		SQL.connect("192.168.0.130", "3306", "BetaRumiServer", "rumiserver_beta", "1234");

		SQLMigrationer.add(
			new MigrationCreateTable("TEST", new MigrationCreateTable.Column[]{
				new MigrationCreateTable.Column("ID", "主キー", new MigrationCreateTable.ColumnType(MigrationCreateTable.ColumnType.Name.Varchar, 256), MigrationCreateTable.ColumnConstraints.PrimaryKey),
				new MigrationCreateTable.Column("CONTENTS", "内容物", new MigrationCreateTable.ColumnType(MigrationCreateTable.ColumnType.Name.Text), MigrationCreateTable.ColumnConstraints.None)
			}, new MigrationCreateTable.ReferenceColumn[0])
		);

		SQLMigrationer.migration("SQL_TEST");
	}
}
