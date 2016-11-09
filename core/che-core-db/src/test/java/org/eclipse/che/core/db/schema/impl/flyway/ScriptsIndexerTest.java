package org.eclipse.che.core.db.schema.impl.flyway;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.internal.util.Location;
import org.flywaydb.core.internal.util.scanner.filesystem.FileSystemResource;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Tests {@link ScriptsIndexer}.
 *
 * @author Yevhenii Voevodin
 */
public class ScriptsIndexerTest {

    private final Flyway flyway = new Flyway();

    @BeforeMethod
    public void setUpDefaults() {
        flyway.setSqlMigrationSuffix(".sql");
        flyway.setSqlMigrationPrefix("");
        flyway.setSqlMigrationSeparator(".");
    }

    @Test
    public void indexesScripts() {
        final SqlScript script1 = new SqlScript(new FileSystemResource("1.init.sql"),
                                                new Location("sql"),
                                                "5.0.0-M1",
                                                null,
                                                "1.sql");
        final SqlScript script2 = new SqlScript(new FileSystemResource("2.sql"),
                                                new Location("sql"),
                                                "5.0.0-M1",
                                                null,
                                                "2.sql");
        final SqlScript script3 = new SqlScript(new FileSystemResource("100.modify.sql"),
                                                new Location("sql"),
                                                "5.0.0-M1",
                                                null,
                                                "100.sql");
        final ListMultimap<String, SqlScript> versionToScripts = ArrayListMultimap.create();
        versionToScripts.put(script1.versionDir, script1);
        versionToScripts.put(script2.versionDir, script2);
        versionToScripts.put(script3.versionDir, script3);

        new ScriptsIndexer().indexScripts(versionToScripts, flyway);

        assertEquals(script1.index, 1);
        assertEquals(script1.migrationVersion, MigrationVersion.fromVersion("5.0.0.1.1"));
        assertEquals(script2.index, 2);
        assertEquals(script2.migrationVersion, MigrationVersion.fromVersion("5.0.0.1.2"));
        assertEquals(script3.index, 3);
        assertEquals(script3.migrationVersion, MigrationVersion.fromVersion("5.0.0.1.3"));
    }

    @Test
    public void ignoresPrefixWhileIndexingScripts() throws Exception {
        flyway.setSqlMigrationPrefix("version-");
        final SqlScript script1 = new SqlScript(new FileSystemResource("version-1.init.sql"),
                                                new Location("sql"),
                                                "5.0.0-M1",
                                                null,
                                                "version-1.init.sql");
        final ListMultimap<String, SqlScript> versionToScripts = ArrayListMultimap.create();
        versionToScripts.put(script1.versionDir, script1);

        new ScriptsIndexer().indexScripts(versionToScripts, flyway);

        assertEquals(script1.index, 1);
        assertEquals(script1.migrationVersion, MigrationVersion.fromVersion("5.0.0.1.1"));
    }

    @Test(expectedExceptions = FlywayException.class)
    public void failsWhenTwoScriptsProvideTheSameVersion() {
        final SqlScript script1 = new SqlScript(new FileSystemResource("1.init.sql"),
                                                new Location("sql"),
                                                "5.0.0-M1",
                                                null,
                                                "1.init.sql");
        final SqlScript script2 = new SqlScript(new FileSystemResource("1.sql"),
                                                new Location("sql"),
                                                "5.0.0-M1",
                                                null,
                                                "1.sql");
        final ListMultimap<String, SqlScript> versionToScripts = ArrayListMultimap.create();
        versionToScripts.put(script1.versionDir, script1);
        versionToScripts.put(script2.versionDir, script2);

        new ScriptsIndexer().indexScripts(versionToScripts, flyway);
    }

    @Test(dataProvider = "invalidScripts", expectedExceptions = FlywayException.class)
    public void failsWhenScriptVersionIsInvalid(String name) {
        final SqlScript script1 = new SqlScript(new FileSystemResource(name),
                                                new Location("sql"),
                                                "5.0.0-M1",
                                                null,
                                                name);
        final ListMultimap<String, SqlScript> versionToScripts = ArrayListMultimap.create();
        versionToScripts.put(script1.versionDir, script1);

        new ScriptsIndexer().indexScripts(versionToScripts, flyway);
    }

    @DataProvider(name = "invalidScripts")
    public Object[][] invalidScripts() {
        return new String[][] {
                {"2016-11-11.init.sql"},
                {"one.init.sql"},
                {".init.sql"}
        };
    }
}
