package su.rumishistem.rumi_java_sql.Migration.Type.MigrationCreateTableType;

import su.rumishistem.rumi_java_sql.SQLC;
import su.rumishistem.rumi_java_sql.Migration.SQLMigrationer;
import su.rumishistem.rumi_java_sql.Migration.Type.MigrationProcess;

/**
 * テーブル作成を行うマイグレーションです
 */
public class MigrationCreateTable extends MigrationProcess{
	private final String table_name;
	private final Column[] column_list;
	private final ReferenceColumn[] reference_list;
	private final UniqueColumn[] unique_list;

	public MigrationCreateTable(String name, Column[] column_list, ReferenceColumn[] reference_list, UniqueColumn[] unique_list) {
		this.table_name = name;
		this.column_list = column_list;

		if (reference_list == null) {
			this.reference_list = new ReferenceColumn[0];
		} else {
			this.reference_list = reference_list;
		}

		if (unique_list == null) {
			this.unique_list = new UniqueColumn[0];
		} else {
			this.unique_list = unique_list;
		}
	}

	public MigrationCreateTable(String name, Column[] column_list, ReferenceColumn[] reference_list) {
		this.table_name = name;
		this.column_list = column_list;

		if (reference_list == null) {
			this.reference_list = new ReferenceColumn[0];
		} else {
			this.reference_list = reference_list;
		}

		this.unique_list = new UniqueColumn[0];
	}

	public MigrationCreateTable(String name, Column[] column_list, UniqueColumn[] unique_list) {
		this.table_name = name;
		this.column_list = column_list;

		this.reference_list = new ReferenceColumn[0];

		if (unique_list == null) {
			this.unique_list = new UniqueColumn[0];
		} else {
			this.unique_list = unique_list;
		}
	}

	public MigrationCreateTable(String name, Column[] column_list) {
		this.table_name = name;
		this.column_list = column_list;
		this.reference_list = new ReferenceColumn[0];
		this.unique_list = new UniqueColumn[0];
	}

	@Override
	public void run(SQLC sql) throws Exception {
		StringBuilder sb = new StringBuilder();
		String primary_key_column_name = null;

		sb.append("CREATE TABLE IF NOT EXISTS `"+table_name+"` (");

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
				sb.append("CONSTRAINT `fk_"+table_name.toLowerCase()+"_"+ref.foreign+"`");
				sb.append(" ");
				sb.append("FOREIGN KEY (`"+ref.foreign+"`)");
				sb.append(" ");
				sb.append("REFERENCES `"+ref.reference_table+"` (`"+ref.reference_column+"`)");

				if (i + 1 < reference_list.length) sb.append(",");
			}
		}

		//ユニーク
		if (unique_list.length != 0) {
			sb.append(",");
			for (int i = 0; i < unique_list.length; i++) {
				UniqueColumn unique = unique_list[i];
				sb.append("UNIQUE KEY");
				sb.append(" ");
				sb.append(unique.name);
				sb.append(" ");
				sb.append("(");
				for (int j = 0; j < unique.column_list.length; j++) {
					String column = unique.column_list[j];
					sb.append("`"+column+"`");
					if (i + 1 < unique.column_list.length) sb.append(",");
				}
				sb.append(")");

				if (i + 1 < unique_list.length) sb.append(",");
			}
		}

		sb.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;");

		String sql_script = sb.toString();

		System.out.println(SQLMigrationer.LOG_PREFIX + "SQL EXEC > ["+sql_script+"]");
		sql.update_execute(sql_script, new Object[0]);
	}
}