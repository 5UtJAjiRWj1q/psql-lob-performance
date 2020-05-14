package test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.Random;

/**
 * Compare LOB access file/bytea/bytea external/lo
 * <p>
 * start PG first:
 * <p>
 * <code>
 * docker run --rm -e POSTGRES_USER=pg -e POSTGRES_PASSWORD=pg -e POSTGRES_DB=pg -p 127.0.0.1:15432:5432 postgres
 * </code>
 */
public class PostgresqlLobPerformance {

	@FunctionalInterface
	interface CheckedBiConsumer<T, U> {
		void accept(T t, U u) throws Exception;
	}

	@FunctionalInterface
	interface CheckedFunction<T, R> {
		R apply(T t) throws Exception;
	}

	@FunctionalInterface
	interface CheckedRunnable {
		void run() throws Exception;
	}

	private static final int TESTCONTENTSIZE = 300 * 1 << 20; // 300MiB
	private static final int ITERATIONS = 5;

	private File file;
	private Random random = new Random();
	private byte[] content = new byte[TESTCONTENTSIZE];
	private byte[] buffer = new byte[4 * 1 << 10]; // 4KiB
	private Connection connection;
	private long lastTime;

	public static void main(String[] args) throws Exception {
		new PostgresqlLobPerformance().run();
	}

	private void log(String s) {
		long time = System.currentTimeMillis();
		if (lastTime != 0L) {
			Duration duration = Duration.ofMillis(time - lastTime);
			System.out.println(duration.toString().substring(2).toLowerCase());
		}
		lastTime = time;
		System.out.print(String.format("%-25s", s));
	}

	private void run() throws Exception {
		try {
			prepare();
			iterate("file", this::readFile);
			iterate("bytea", this::readBytea);
			iterate("bytea_external", this::readbytea_external);
			iterate("lo", this::readLob);
		} finally {
			log("close connection");
			file.delete();
			connection.close();
		}
		log("done");
	}

	private void prepare() throws Exception {
		log("create content");
		random.nextBytes(content);

		log("open connection");
		connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:15432/pg", "pg", "pg");
		connection.setAutoCommit(true);
		log("create structure");
		try (Statement s = connection.createStatement()) {
			s.executeUpdate("CREATE EXTENSION IF NOT EXISTS lo");
			s.executeUpdate("CREATE TABLE IF NOT EXISTS testbytea ( blobfield bytea )");
			s.executeUpdate("CREATE TABLE IF NOT EXISTS testbytea_external ( blobfield bytea )");
			s.executeUpdate("CREATE TABLE IF NOT EXISTS testlo ( blobfield lo )");
			s.executeUpdate("ALTER TABLE testbytea_external ALTER COLUMN blobfield SET STORAGE EXTERNAL");
			s.executeUpdate("TRUNCATE testbytea");
			s.executeUpdate("TRUNCATE testbytea_external");
			s.executeUpdate("TRUNCATE testlo");
		}
		connection.setAutoCommit(false);

		writeFile();
		writeBytea();
		writebytea_external();
		writeLob();

		log("commit");
		connection.commit();
	}

	private void writeDB(String type, CheckedBiConsumer<PreparedStatement, InputStream> setter) throws Exception {
		log("write " + type);
		try (PreparedStatement p = connection.prepareStatement("INSERT INTO " + type + " VALUES (?)")) {
			setter.accept(p, new ByteArrayInputStream(content));
			p.executeUpdate();
		}
	}

	private void writeLob() throws Exception {
		writeDB("testlo", (p, i) -> p.setBlob(1, i));
	}

	private void writeBytea() throws Exception {
		writeDB("testbytea", (p, i) -> p.setBinaryStream(1, i));
	}

	private void writebytea_external() throws Exception {
		writeDB("testbytea_external", (p, i) -> p.setBinaryStream(1, i));
	}

	private void writeFile() throws IOException {
		log("create file");
		file = File.createTempFile("logtest", ".tmp");
		try (FileOutputStream out = new FileOutputStream(file)) {
			out.write(content);
		}
	}

	private void readStream(InputStream stream) throws IOException {
		try {
			while (true) {
				int i = stream.read(buffer);
				if (i < 0) {
					break;
				}
			}
		} finally {
			stream.close();
		}
	}

	private void iterate(String type, CheckedRunnable method) throws Exception {
		log("read " + type);
		long time = System.currentTimeMillis();
		for (int i = 0; i < ITERATIONS; i++) {
			method.run();
		}
		time = System.currentTimeMillis() - time;
		System.out.print(MessageFormat.format("{0,number,#.#} MiB/s  ",
				new Object[] { Double.valueOf(1000. * ITERATIONS * TESTCONTENTSIZE / time / (1 << 20)) }));
	}

	private void readFile() throws Exception {
		try (FileInputStream in = new FileInputStream(file)) {
			readStream(in);
		}
	}

	private void read(String table, CheckedFunction<ResultSet, InputStream> getter) throws Exception {
		try (PreparedStatement p = connection.prepareStatement("SELECT * FROM " + table)) {
			try (ResultSet r = p.executeQuery()) {
				r.next();
				readStream(getter.apply(r));
			}
		}
	}

	private void readBytea() throws Exception {
		read("testbytea", r -> r.getBinaryStream(1));
	}

	private void readbytea_external() throws Exception {
		read("testbytea_external", r -> r.getBinaryStream(1));
	}

	private void readLob() throws Exception {
		read("testlo", r -> r.getBlob(1).getBinaryStream());
	}

}
