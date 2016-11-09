package org.eclipse.che.core.db.schema.impl.flyway;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.internal.util.Location;
import org.flywaydb.core.internal.util.scanner.classpath.ClassPathResource;
import org.flywaydb.core.internal.util.scanner.filesystem.FileSystemResource;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Tests {@link ScriptsFinder}.
 *
 * @author Yevhenii Voevodin
 */
public class ScriptsFinderTest {

    private final List<Path> cleanAfter = new ArrayList<>();
    private final Flyway     flyway     = new Flyway();

    @BeforeMethod
    public void setUpDefaults() {
        flyway.setSqlMigrationSuffix(".sql");
        flyway.setSqlMigrationPrefix("");
    }

    @AfterMethod
    public void cleanup() throws IOException {
        for (Path path : cleanAfter) {
            Files.delete(path);
        }
        cleanAfter.clear();
    }

    @Test
    public void findsScriptsInClassPath() throws Exception {
        flyway.setLocations("classpath:finder-sql-files");
        cleanAfter.addAll(createFiles("finder-sql-files/1.0/1.sql",
                                      "finder-sql-files/1.0/2.sql",
                                      "finder-sql-files/2.0/1.sql",
                                      "finder-sql-files/2.0/postgresql/1.sql"));

        final ListMultimap<String, SqlScript> scripts = new ScriptsFinder().findScripts(flyway);

        final ClassLoader cl = flyway.getClassLoader();
        assertEquals(new HashSet<>(scripts.values()),
                     Sets.newHashSet(new SqlScript(new ClassPathResource("finder-sql-files/1.0/1.sql", cl),
                                                   new Location("finder-sql-files"),
                                                   "1.0",
                                                   null,
                                                   "1.sql"),
                                     new SqlScript(new ClassPathResource("finder-sql-files/1.0/2.sql", cl),
                                                   new Location("finder-sql-files"),
                                                   "1.0",
                                                   null,
                                                   "2.sql"),
                                     new SqlScript(new ClassPathResource("finder-sql-files/2.0/1.sql", cl),
                                                   new Location("finder-sql-files"),
                                                   "2.0",
                                                   null,
                                                   "1.sql"),
                                     new SqlScript(new ClassPathResource("finder-sql-files/2.0/postgresql/1.sql", cl),
                                                   new Location("finder-sql-files"),
                                                   "2.0",
                                                   "postgresql",
                                                   "1.sql")));
    }

    @Test
    public void findsScriptsOnFileSystem() throws Exception {
        final List<Path> paths = createFiles("finder-sql-files/1.0/1.sql",
                                             "finder-sql-files/1.0/2.sql",
                                             "finder-sql-files/2.0/1.sql",
                                             "finder-sql-files/2.0/postgresql/1.sql");
        cleanAfter.addAll(paths);
        final Path finderSqlFiles = paths.get(0).getParent().getParent();
        final String fsLocation = "filesystem:" + finderSqlFiles.toAbsolutePath();
        flyway.setLocations(fsLocation);

        final ListMultimap<String, SqlScript> scripts = new ScriptsFinder().findScripts(flyway);

        assertEquals(new HashSet<>(scripts.values()),
                     Sets.newHashSet(new SqlScript(new FileSystemResource(finderSqlFiles.resolve("1.0")
                                                                                        .resolve("1.sql")
                                                                                        .toString()),
                                                   new Location(fsLocation),
                                                   "1.0",
                                                   null,
                                                   "1.sql"),
                                     new SqlScript(new FileSystemResource(finderSqlFiles.resolve("1.0")
                                                                                        .resolve("2.sql")
                                                                                        .toString()),
                                                   new Location(fsLocation),
                                                   "1.0",
                                                   null,
                                                   "2.sql"),
                                     new SqlScript(new FileSystemResource(finderSqlFiles.resolve("2.0")
                                                                                        .resolve("1.sql")
                                                                                        .toString()),
                                                   new Location(fsLocation),
                                                   "2.0",
                                                   null,
                                                   "1.sql"),
                                     new SqlScript(new FileSystemResource(finderSqlFiles.resolve("2.0")
                                                                                        .resolve("postgresql")
                                                                                        .resolve("1.sql")
                                                                                        .toString()),
                                                   new Location(fsLocation),
                                                   "2.0",
                                                   "postgresql",
                                                   "1.sql")));
    }

    @Test
    public void findsFileSystemAndClassPathScripts() throws Exception {
        final List<Path> paths = createFiles("finder-fs-sql-files/1.0/1.sql",
                                             "finder-fs-sql-files/2.0/2.sql",
                                             "finder-cp-sql-files/1.0/2.sql",
                                             "finder-cp-sql-files/2.0/postgresql/1.sql");
        cleanAfter.addAll(paths);
        final Path finderFsSqlFiles = paths.get(0).getParent().getParent();
        final String fsLocation = "filesystem:" + finderFsSqlFiles.toAbsolutePath();
        final String cpLocation = "classpath:finder-cp-sql-files";
        flyway.setLocations(fsLocation, cpLocation);

        final ListMultimap<String, SqlScript> scripts = new ScriptsFinder().findScripts(flyway);

        final ClassLoader cl = flyway.getClassLoader();
        assertEquals(new HashSet<>(scripts.values()),
                     Sets.newHashSet(new SqlScript(new FileSystemResource(finderFsSqlFiles.resolve("1.0")
                                                                                          .resolve("1.sql")
                                                                                          .toString()),
                                                   new Location(fsLocation),
                                                   "1.0",
                                                   null,
                                                   "1.sql"),
                                     new SqlScript(new FileSystemResource(finderFsSqlFiles.resolve("2.0")
                                                                                          .resolve("2.sql")
                                                                                          .toString()),
                                                   new Location(fsLocation),
                                                   "2.0",
                                                   null,
                                                   "2.sql"),
                                     new SqlScript(new ClassPathResource("finder-cp-sql-files/1.0/2.sql", cl),
                                                   new Location("finder-cp-sql-files"),
                                                   "1.0",
                                                   null,
                                                   "2.sql"),
                                     new SqlScript(new ClassPathResource("finder-cp-sql-files/2.0/postgresql/1.sql", cl),
                                                   new Location("finder-cp-sql-files"),
                                                   "2.0",
                                                   "postgresql",
                                                   "1.sql")));
    }

    private static List<Path> createFiles(String... paths) throws URISyntaxException, IOException {
        final URL url = Thread.currentThread().getContextClassLoader().getResource(".");
        assertNotNull(url);
        final Path classesDir = Paths.get(url.toURI());
        final List<Path> createdFiles = new ArrayList<>(paths.length);
        for (String stringPath : paths) {
            final Path path = classesDir.resolve(Paths.get(stringPath));
            if (!Files.exists(path.getParent())) {
                Files.createDirectories(path.getParent());
            }
            Files.write(path, path.toString().getBytes(StandardCharsets.UTF_8));
            createdFiles.add(path);
        }
        return createdFiles;
    }
}
