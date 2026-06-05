package su.rumishistem.rumi_java_sql.Migration.Type;

import su.rumishistem.rumi_java_sql.SQLC;
import su.rumishistem.rumi_java_sql.Migration.SQLMigrationer;

/**
 * テーブル作成を行うマイグレーションです
 */
public class MigrationCreateTable extends MigrationProcess{
	public static class ColumnType {
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

	public static enum ColumnConstraints {
		None,
		PrimaryKey
	}

	public static class Column {
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

	public static class NullableColumn extends Column {
		public NullableColumn(String name, String description, ColumnType type, ColumnConstraints constraints) {
			super(name, description, type, constraints);
		}
	}

	public static class ReferenceColumn {
		public final String foreign;
		public final String reference_table;
		public final String reference_column;

		public ReferenceColumn(String foreign, String reference_table, String reference_column) {
			this.foreign = foreign;
			this.reference_table = reference_table;
			this.reference_column = reference_column;
		}
	}

	private final String sql_script;

	public MigrationCreateTable(String name, Column[] column_list, ReferenceColumn[] reference_list) {
		StringBuilder sb = new StringBuilder();
		String primary_key_column_name = null;

		sb.append("CREATE TABLE `"+name+"` (");

		for (Column col:column_list) {
			//カラム名
			sb.append("`"+col.name+"`");
			sb.append(" ");

			//型
			sb.append(col.type.name.name().toUpperCase());
			if (col.type.size != -1) {
				sb.append("("+col.type.size+")");
			}
			sb.append(" ");

			//カラム制約
			if (col.constraints == ColumnConstraints.PrimaryKey) {
				primary_key_column_name = col.name;
				sb.append("PRIMARY KEY");
				sb.append(" ");
			}

			//Null
			if (!(col instanceof NullableColumn)) {
				sb.append("NOT NULL");
				sb.append(" ");
			}

			//説明
			sb.append("COMMENT '"+col.description+"'");
			sb.append(" ");

			//区切り
			sb.append(",");
		}

		//インデックス
		if (primary_key_column_name == null) throw new IllegalStateException("主キーが無いテーブルとかふざけているのか？出直せ");
		sb.append("INDEX `idx_"+primary_key_column_name.toLowerCase()+"` (`"+primary_key_column_name+"`)");

		//外部キー制約
		if (reference_list.length != 0) {
			sb.append(",");
			for (int i = 0; i < reference_list.length; i++) {
				ReferenceColumn ref = reference_list[i];
				//CONSTRAINT `fk_rumipass_site_user` FOREIGN KEY (`USER`) REFERENCES `ACCOUNT`(`ID`)
				sb.append("CONSTRAINT `fk_"+name.toLowerCase()+"_"+ref.foreign+"`");
				sb.append(" ");
				sb.append("FOREIGN KEY (`"+ref.foreign+"`)");
				sb.append(" ");
				sb.append("REFERENCES `"+ref.reference_table+"` (`"+ref.reference_column+"`)");

				if (i + 1 < reference_list.length) sb.append(",");
			}
		}

		sb.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;");

		this.sql_script = sb.toString();
	}

	@Override
	public void run(SQLC sql) throws Exception {
		System.out.println(SQLMigrationer.LOG_PREFIX + "SQL EXEC > ["+sql_script+"]");
		sql.update_execute(sql_script, new Object[0]);
	}
}