package su.rumishistem.rumi_java_sql.Migration;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import su.rumishistem.rumi_java_sql.SQL;
import su.rumishistem.rumi_java_sql.SQLC;
import su.rumishistem.rumi_java_sql.SQLValue;
import su.rumishistem.rumi_java_sql.Migration.Type.MigrationProcess;

public class SQLMigrationer {
	public static final String LOG_PREFIX = "[  MIGRATION  ] ";

	private static List<MigrationProcess[]> migration_list = new ArrayList<>();

	public static void add(MigrationProcess... process) {
		migration_list.add(process);
	}

	public static void migration(String service_name) throws SQLException {
		final String META_TABLE_NAME = service_name + "_VERSION";
		final int LAST_VERSION = migration_list.size();
		int NOW_VERSION;

		SQLC sql = SQL.new_connection();

		//テーブルが初回状態か
		Map<String, SQLValue>[] meta = sql.select_execute("SELECT * FROM `META` WHERE `NAME` = ?;", new Object[]{META_TABLE_NAME});
		if (meta.length == 0) {
			sql.update_execute("INSERT INTO `META` (`NAME`, `VALUE`) VALUES (?, '0');", new Object[]{META_TABLE_NAME});
			NOW_VERSION = 0;

			System.out.println(LOG_PREFIX + "METAﾃｰﾌﾞﾙにSQLﾊﾞｰｼﾞｮﾝを挿入しました。");
		} else {
			NOW_VERSION = Integer.parseInt(meta[0].get("VALUE").as_string());
			System.out.println(LOG_PREFIX + "SQLﾊﾞｰｼﾞｮﾝ: " + NOW_VERSION);
		}

		if (LAST_VERSION > NOW_VERSION) {
			System.out.println(LOG_PREFIX + "ﾏｲｸﾞﾚｰｼｮﾝを開始します！ " + NOW_VERSION + "から" + LAST_VERSION + "まで");

			try {
				for (int v = NOW_VERSION; v < LAST_VERSION; v++) {
					MigrationProcess[] process_list = migration_list.get(v);
					System.out.println(LOG_PREFIX + "ﾊﾞｰｼﾞｮﾝ" + v + "のﾏｲｸﾞﾚｰｼｮﾝ...");
					for (MigrationProcess process:process_list) {
						process.run(sql);
					}
				}

				//バージョンアップ
				sql.update_execute("UPDATE `META` SET `VALUE` = ? WHERE `NAME` = ?; ", new Object[]{LAST_VERSION, META_TABLE_NAME});

				sql.commit();
				sql.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(LOG_PREFIX + "ﾏｲｸﾞﾚｰｼｮﾝ失敗: " + e.getMessage());

				sql.rollback();
				sql.close();
				System.exit(1);
			}
		} else {
			sql.close();
			System.out.println(LOG_PREFIX + "ﾏｲｸﾞﾚｰｼｮﾝの必要はありません。");
		}
	}
}
