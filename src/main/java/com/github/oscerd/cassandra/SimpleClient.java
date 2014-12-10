package com.github.oscerd.cassandra;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Ordering;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class SimpleClient {
	private Cluster cluster;
	private Session session;

	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		System.out.println("Connected to cluster:" + metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.println("Datatacenter: " + host.getDatacenter()
					+ "; Host: " + host.getAddress() + "; Rack: "
					+ host.getRack());
		}
	}

	public void getSession() {
		session = cluster.connect();
	}

	public void closeSession() {
		session.close();
	}

	public void close() {
		cluster.close();
	}

	public void createSchema() {
		session.execute("CREATE KEYSPACE IF NOT EXISTS simplex WITH replication "
				+ "= {'class':'SimpleStrategy', 'replication_factor':3};");
		session.execute("CREATE TABLE IF NOT EXISTS simplex.songs ("
				+ "id uuid PRIMARY KEY," + "title text," + "album text,"
				+ "artist text," + "tags set<text>," + "data blob" + ");");
		session.execute("CREATE TABLE IF NOT EXISTS simplex.playlists ("
				+ "id uuid," + "title text," + "album text, " + "artist text,"
				+ "song_id uuid," + "PRIMARY KEY (id, title, album, artist)"
				+ ");");
	}

	public void loadData() {

		PreparedStatement statement = session
				.prepare("INSERT INTO simplex.songs "
						+ "(id, title, album, artist, tags) "
						+ "VALUES (?, ?, ?, ?, ?);");

		BoundStatement boundStatement = new BoundStatement(statement);
		Set<String> tags = new HashSet<String>();
		tags.add("metal");
		tags.add("1992");
		UUID idAlbum = UUID.randomUUID();
		ResultSet res = session.execute(boundStatement.bind(
				idAlbum,
				"DNR", "The gathering",
				"Testament", tags));
		
		System.err.println(res.toString());
		
		statement = session.prepare("INSERT INTO simplex.playlists "
				+ "(id, song_id, title, album, artist) "
				+ "VALUES (?, ?, ?, ?, ?);");
		
		UUID idPlaylist = UUID.randomUUID();
		boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind(
				idPlaylist,
				idAlbum,
				"DNR", "The gathering", "Testament"));
	}

	public void querySchema() {
		Statement statement = QueryBuilder.select().all().from("simplex", "songs");
		ResultSet results = session.execute(statement);
		System.out
				.println(String
						.format("%-50s\t%-30s\t%-20s\t%-20s\n%s", "id", "title", "album",
								"artist",
								"------------------------------------------------------+-------------------------------+------------------------+-----------"));
		for (Row row : results) {
			System.out.println(String.format("%-50s\t%-30s\t%-20s\t%-20s",
					row.getUUID("id"), row.getString("title"), row.getString("album"),
					row.getString("artist")));
		}
		System.out.println();
	}
}
