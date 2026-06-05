package su.rumishistem.rumi_java_sql.Migration.Type;

import su.rumishistem.rumi_java_sql.SQLC;
import su.rumishistem.rumi_java_sql.Migration.SQLMigrationer;

/**
 * SQL文を直接実行するマイグレーションです
 */
public class MigrationSQLSCript extends MigrationProcess{
	private final String script;

	/**
	 * SQL文を直接実行するマイグレーションです
	 * @param script SQL文
	 */
	public MigrationSQLSCript(String script) {
		this.script = script;
	}

	@Override
	public void run(SQLC sql) throws Exception {
		System.out.println(SQLMigrationer.LOG_PREFIX + "SQL EXEC > ["+script+"]");
		sql.update_execute(script, new Object[0]);
	}
}
