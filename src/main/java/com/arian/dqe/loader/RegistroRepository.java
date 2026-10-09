package com.arian.dqe.loader;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Solo lectura por cursor sobre registro (D1). Usuario de base con SELECT únicamente.
 * N se estima con MAX(id), nunca COUNT(*).
 */
@Repository
public class RegistroRepository {

	public record Row(long id, String providerId, String endpoint) {
	}

	private final JdbcTemplate jdbc;

	public RegistroRepository(JdbcTemplate jdbc) {
		this.jdbc = jdbc;
	}

	public long maxId() {
		Long max = jdbc.queryForObject("SELECT MAX(id) FROM registro", Long.class);
		return max == null ? 0 : max;
	}

	public List<Row> after(long cursor, int limit) {
		return jdbc.query("SELECT id, provider_id, endpoint FROM registro WHERE id > ? ORDER BY id LIMIT ?",
				(rs, i) -> new Row(rs.getLong("id"), rs.getString("provider_id"), rs.getString("endpoint")), cursor, limit);
	}
}
