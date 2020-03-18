/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.tests.util;

import org.apache.flink.util.Preconditions;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * General test utilities.
 */
public enum TestUtils {
	;

	/**
	 * Searches for a jar matching the given regex in the given directory. This method is primarily intended to be used
	 * for the initialization of static {@link Path} fields for jars that reside in the modules {@code target} directory.
	 *
	 * @param jarNameRegex      regex pattern to match against
	 * @return Path pointing to the matching jar
	 * @throws RuntimeException if none or multiple jars could be found
	 */
	public static Path getResourceJar(final String jarNameRegex) {
		String moduleDirProp = System.getProperty("moduleDir");
		Preconditions.checkNotNull(moduleDirProp, "The moduleDir property was not set, You can set it when running maven via -DmoduleDir=<path>");

		try (Stream<Path> dependencyJars = Files.walk(Paths.get(moduleDirProp))) {
			final List<Path> matchingJars = dependencyJars
				.filter(jar -> Pattern.compile(jarNameRegex).matcher(jar.toAbsolutePath().toString()).find())
				.collect(Collectors.toList());
			switch (matchingJars.size()) {
				case 0:
					throw new RuntimeException(
						new FileNotFoundException(
							String.format("No jar could be found that matches the pattern %s.", jarNameRegex)
						)
					);
				case 1:
					return matchingJars.get(0);
				default:
					throw new RuntimeException(
						new IOException(
							String.format("Multiple jars were found matching the pattern %s. Matches=%s", jarNameRegex, matchingJars)
						)
					);
			}
		} catch (final IOException ioe) {
			throw new RuntimeException("Could not search for resource jars.", ioe);
		}
	}

	/**
	 * Copy all the files and sub-directories under source directory to destination directory recursively.
	 *
	 * @param source      directory or file path to copy from.
	 * @param destination directory or file path to copy to.
	 * @return Path of the destination directory.
	 * @throws IOException if any IO error happen.
	 */
	public static Path copyDirectory(final Path source, final Path destination) throws IOException {
		Files.walkFileTree(source, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes ignored)
				throws IOException {
				final Path targetDir = destination.resolve(source.relativize(dir));
				try {
					Files.copy(dir, targetDir, StandardCopyOption.COPY_ATTRIBUTES);
				} catch (FileAlreadyExistsException e) {
					if (!Files.isDirectory(targetDir)) {
						throw e;
					}
				}
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes ignored) throws IOException {
				Files.copy(file, destination.resolve(source.relativize(file)), StandardCopyOption.COPY_ATTRIBUTES);
				return FileVisitResult.CONTINUE;
			}
		});

		return destination;
	}

	public static Connection createConn(String ip, int port)  {
		Connection conn = new Connection(ip, port);
		return conn;
	}

	public static SCPClient createSCPClient(Connection conn, String user, String passwd) throws IOException{
		try {
			conn.connect();
			boolean isAuthenticated = conn.authenticateWithPassword(user, passwd);
			if (!isAuthenticated){
				throw new IOException("Authentication failed.");
			}
			SCPClient client = new SCPClient(conn);
			return client;
		} catch (IOException e) {
			throw e;
		}
	}

	/**
	 * Remote copy all the files and sub-directories under source directory
	 * from local to destination directory of remote machines recursively.
	 *
	 * @param source      directory or file path to copy from.
	 * @param destination directory or file path to copy to.
	 * @param slaves machines to copy to
	 * @return Path of the destination directory.
	 * @throws IOException if any IO error happen.
	 */
	public static String remoteCopyDirectory(
		String source, String destination, String slaves, int port, String user, String passwd) throws IOException {
		String[] ips = slaves.split(",");
		for (String ip : ips) {
			try {
				Connection conn = createConn(ip, port);
				SCPClient client = createSCPClient(conn, user, passwd);
				client.put(source, destination);
				conn.close();
			} catch (IOException e) {
				throw e;
			}
		}

		return destination;
	}

	/**
	 * Remote copy all the files and sub-directories under source directory
	 * from remote machines to local destination directory recursively.
	 *
	 * @param source      directory or file path to copy from.
	 * @param destination directory or file path to copy to.
	 * @param slaves machines to copy from
	 * @return Path of the destination directory.
	 * @throws IOException if any IO error happen.
	 */
	public static String remoteGetDirectory(
		String source, String destination, String slaves, int port, String user, String passwd) throws IOException {
		String[] ips = slaves.split(",");
		for (String ip : ips) {
			try {
				Connection conn = createConn(ip, port);
				SCPClient client = createSCPClient(conn, user, passwd);
				client.get(source, destination);
				conn.close();
			} catch (IOException e) {
				throw e;
			}
		}

		return destination;
	}
}
