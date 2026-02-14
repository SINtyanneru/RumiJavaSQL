package su.rumishistem.rumi_java_sql;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLC implements AutoCloseable{
	private Connection connection;
	private boolean auto_commit = false;

	public SQLC(Connection connection, boolean auto_commit) {
		this.connection = connection;
		this.auto_commit = auto_commit;
	}

	public void update_execute(String script, Object[] param_list) throws SQLException {
		PreparedStatement stmt = connection.prepareStatement(script);

		try {
			stmt_set_param(stmt, param_list);
			stmt.executeUpdate();
		} finally {
			if (stmt != null) stmt.close();
			if (auto_commit) {
				commit();
				close();
			}
		}
	}

	@SuppressWarnings("unchecked")
	public Map<String, SQLValue>[] select_execute(String script, Object[] param_list) throws SQLException{
		PreparedStatement stmt = null;
		ResultSet result = null;

		try {
			stmt = connection.prepareStatement(script);
			stmt_set_param(stmt, param_list);

			result = stmt.executeQuery();
			ResultSetMetaData meta = result.getMetaData();
			final int col_count = meta.getColumnCount();

			List<Map<String, SQLValue>> temp = new ArrayList<>();
			while (result.next()) {
				Map<String, SQLValue> row = new HashMap<>();
				for (int i = 1; i <= col_count; i++) {
					String name = meta.getColumnLabel(i);
					SQLValue value = new SQLValue(result.getObject(i));
					row.put(name, value);
				}

				temp.add(row);
			}

			Map<String, SQLValue>[] r = (Map<String, SQLValue>[]) new Map[temp.size()];
			for (int i = 0; i < r.length; i++) {
				Map<String, SQLValue> row = temp.get(i);
				r[i] = row;
			}
			return r;
		} finally {
			if (result != null) result.close();
			if (stmt != null) stmt.close();
			if (auto_commit) {
				commit();
				close();
			}
		}
	}

	private void stmt_set_param(PreparedStatement stmt, Object[] param_list) throws SQLException {
		for (int i = 0; i < param_list.length; i++) {
			Object param = param_list[i];

			if (param == null) {
				stmt.setNull(i+1, Types.NULL);
				continue;
			}

			Class<?> c = param.getClass();
			if (c == String.class) {
				stmt.setString(i+1, (String) param);
			} else if (c == Integer.class) {
				stmt.setInt(i+1, (Integer) param);
			} else if (c == Long.class) {
				stmt.setLong(i+1, (Long) param);
			} else if (c == Boolean.class) {
				stmt.setBoolean(i+1, (Boolean) param);
			} else if (c == byte[].class) {
				stmt.setBytes(i+1, (byte[]) param);
			} else if (c == Byte.class) {
				stmt.setByte(i+1, (Byte) param);
			} else {
				throw new RuntimeException(c.getSimpleName() + "という型は未対応です");
			}
		}
	}

	public void begin() throws SQLException {
		//無いと違和感しか無いので置いてあるだけの関数
	}

	public void commit() throws SQLException {
		connection.commit();
	}

	public void rollback() throws SQLException {
		connection.rollback();
	}

	@Override
	public void close() throws SQLException {
		connection.close();
	}
}
