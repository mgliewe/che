/*******************************************************************************
 * Copyright (c) 2012-2016 Codenvy, S.A.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *   Codenvy, S.A. - initial API and implementation
 *******************************************************************************/
package org.eclipse.che.core.db.schema.impl.flyway;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.resolver.BaseMigrationResolver;
import org.flywaydb.core.api.resolver.ResolvedMigration;
import org.flywaydb.core.internal.resolver.ResolvedMigrationImpl;
import org.flywaydb.core.internal.util.Location;
import org.flywaydb.core.internal.util.scanner.Resource;
import org.flywaydb.core.internal.util.scanner.classpath.ClassPathScanner;
import org.flywaydb.core.internal.util.scanner.filesystem.FileSystemScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static java.lang.String.format;

/**
 * Resolves SQL migrations from the configured directory,
 * allows overriding of default scripts with database specific ones.
 *
 * <ul>Migrations script must follow the next rules:
 * <li>It must be placed in the project versionDir directory e.g. <i>5.0.1</i></li>
 * <li>Project versionDir directory must be placed in dedicated directory e.g. <i>resources/sql</i></li>
 * <li>Migration/Initialization script name must start with a number e.g <i>1.init.sql</i>,
 * this number indicates the subversion of the database migration, e.g. for versionDir <i>5.0.0</i>
 * and migration script <i>1.init.sql</i> database migration versionDir will be <i>5.0.0.1</i></li>
 * <li>If a directory is not a versionDir directory but it has to be present in migrations root,
 * then it should be included to ignored dirs list</li>
 * <li>If a file is not a part of migration it shouldn't end with migration prefix e.g. <i>.sql</i>
 * then resolver will ignore it</li>
 * </ul>
 *
 * <p>From the structure:
 * <pre>
 *   resources/
 *     /sql
 *       /5.0.0
 *         1.init.sql
 *       /5.0.0-M1
 *         1.rename_fields.sql
 *         2.add_workspace_constraint.sql
 *         /postgresql
 *           2.add_workspace_constraint.sql
 *       /5.0.1
 *         1.stacks_migration.sql
 * </pre>
 *
 * <ul>4 database migrations will be resolved
 * <li>5.0.0.1 - initialization script based on file <i>sql/5.0.0/1.init.sql</i></li>
 * <li>5.0.0.1.1 - modification script based on file <i>sql/5.0.0-M1/1.rename_fields.sql</i></li>
 * <li>5.0.0.1.2 - modification script(if postgresql is current provider) based on file
 * <i>sql/5.0.0-M1/postgresql/2.add_workspace_constraint.sql</li>
 * <li>5.0.1.1 - modification script based on file <i>sql/5.0.1/1.stacks_migrations.sql</i></li>
 * </ul>
 *
 * @author Yevhenii Voevodin
 */
public class CustomSqlMigrationResolver extends BaseMigrationResolver {

    private static final Logger  LOG                       = LoggerFactory.getLogger(CustomSqlMigrationResolver.class);
    private static final String  DEFAULT_VENDOR_NAME       = "default";
    private static final Pattern NOT_VERSION_CHARS_PATTERN = Pattern.compile("[^0-9.]");

    private final String vendorName;

    public CustomSqlMigrationResolver(String dbProviderName) {
        this.vendorName = dbProviderName;
    }

    @Override
    public Collection<ResolvedMigration> resolveMigrations() {
        try {
            return resolveSqlMigrations();
        } catch (IOException | SQLException x) {
            throw new RuntimeException(x.getLocalizedMessage(), x);
        }
    }

    private List<ResolvedMigration> resolveSqlMigrations() throws IOException, SQLException {
        LOG.info("Searching for sql scripts in locations {}", Arrays.toString(flywayConfiguration.getLocations()));
        final ListMultimap<String, SqlScript> scripts = findScripts();

        // filter sql scripts according to current db provider
        final List<SqlScript> pickedScripts = new ArrayList<>();
        for (String name : scripts.keySet()) {
            final List<SqlScript> candidates = scripts.get(name);
            final Map<String, SqlScript> vendorToScript = new HashMap<>();
            for (SqlScript candidate : candidates) {
                final String vendorName = candidate.vendor == null ? DEFAULT_VENDOR_NAME : candidate.vendor;
                final SqlScript previous = vendorToScript.put(vendorName, candidate);
                if (previous != null) {
                    throw new IllegalStateException(format("More than one script with name '%s' is registered for " +
                                                           "database vendor '%s', script '%s' conflicts with '%s'",
                                                           candidate.name,
                                                           vendorName,
                                                           candidate,
                                                           previous));
                }
            }
            SqlScript pickedScript = vendorToScript.get(vendorName);
            if (pickedScript == null) {
                pickedScript = vendorToScript.get(DEFAULT_VENDOR_NAME);
            }
            if (pickedScript != null) {
                pickedScripts.add(pickedScript);
            }
        }

        final List<ResolvedMigration> migrations = new ArrayList<>(pickedScripts.size());
        for (SqlScript script : pickedScripts) {
            // 5.0.0-M1 -> 5.0.0.M1
            final String noDashesVersion = script.versionDir.replace("-", ".");
            // 5.0.0.M1 -> 5.0.0.1
            final String version = NOT_VERSION_CHARS_PATTERN.matcher(noDashesVersion).replaceAll("");

            // TODO
        }
        return migrations;
    }

    private ListMultimap<String, SqlScript> findScripts() throws IOException {
        final ClassPathScanner cpScanner = new ClassPathScanner(flywayConfiguration.getClassLoader());
        final FileSystemScanner fsScanner = new FileSystemScanner();
        final ListMultimap<String, SqlScript> scripts = ArrayListMultimap.create();
        for (String rawLocation : flywayConfiguration.getLocations()) {
            final Location location = new Location(rawLocation);
            final Resource[] resources;
            if (location.isClassPath()) {
                resources = cpScanner.scanForResources(location,
                                                       flywayConfiguration.getSqlMigrationPrefix(),
                                                       flywayConfiguration.getSqlMigrationSuffix());
            } else {
                resources = fsScanner.scanForResources(location,
                                                       flywayConfiguration.getSqlMigrationPrefix(),
                                                       flywayConfiguration.getSqlMigrationSuffix());
            }
            for (Resource resource : resources) {
                final SqlScript script = SqlScript.create(resource, location);
                scripts.put(script.name, script);
            }
        }
        return scripts;
    }

//    private List<ResolvedMigration> resolveSqlMigrationsOld() throws IOException, SQLException {
//        final List<Path> versionDirs = Files.list(null)
//                                            .filter(dir -> Files.isDirectory(dir))
//                                            .collect(Collectors.toList());
//        final List<ResolvedMigration> migrations = new ArrayList<>(versionDirs.size());
//        for (Path versionDir : versionDirs) {
//            final String versionDirName = versionDir.getFileName().toString();
//            final TreeMap<Integer, Path> scripts = findScriptsOld(versionDir);
//            for (Map.Entry<Integer, Path> entry : scripts.entrySet()) {
//                final int scriptVersion = entry.getKey();
//                final Path scriptPath = entry.getValue();
//                // 5.0.0-M1 becomes -> 5.0.0.1
//                // 6.0.0    becomes -> 6.0.0
//                final String versionDir = NOT_VERSION_CHARS_PATTERN.matcher(versionDirName.replaceAll("-", ".")).replaceAll("");
//                final ResolvedMigrationImpl migration = new ResolvedMigrationImpl();
//                // 6.0.0    becomes -> 6.0.0.1 e.g. for 1.init.sql
//                // 6.0.0    becomes -> 6.0.0.2 e.g. for 2.rename_fields.sql
//                migration.setVersion(MigrationVersion.fromVersion(versionDir + "." + scriptVersion));
//                migration.setPhysicalLocation(scriptPath.toAbsolutePath().toString());
//                migration.setScript(migration.getPhysicalLocation());
//                migration.setType(MigrationType.SQL);
//                migration.setDescription(versionDirName);
//                migration.setChecksum(com.google.common.io.Files.hash(scriptPath.toFile(), Hashing.crc32()).asInt());
//                final Connection connection = flywayConfiguration.getDataSource().getConnection();
//                migration.setExecutor(new SqlMigrationExecutor(DbSupportFactory.createDbSupport(connection, true),
//                                                               new FileSystemResource(migration.getPhysicalLocation()),
//                                                               PlaceholderReplacer.NO_PLACEHOLDERS,
//                                                               flywayConfiguration.getEncoding()));
//                migrations.add(migration);
//            }
//        }
//        return migrations;
//    }

    /** Describes sql script either on fs or as resource. */
    private static class SqlScript {

        static SqlScript create(Resource resource, Location location) {
            final String separator = location.isClassPath() ? "/" : File.separator;
            final String relLocation = resource.getLocation().substring(location.getPath().length() + 1);
            final String[] paths = relLocation.split(separator);

            // { "5.0.0-M1", "1.init.sql" }
            if (paths.length == 2) {
                return new SqlScript(resource, location, paths[0], null, paths[1]);
            }

            // { "5.0.0-M1", "postgresql", "1.init.sql" }
            if (paths.length == 3) {
                return new SqlScript(resource, location, paths[0], paths[1], paths[2]);
            }

            throw new IllegalArgumentException(format("Sql script location must be either in 'location-root/versionDir' " +
                                                      "or in 'location-root/versionDir/provider-name', but script '%s' is " +
                                                      "not in that kind of relation with root '%s'",
                                                      resource.getLocation(),
                                                      location.getPath()));
        }

        final Resource resource;
        final Location location;
        final String   versionDir;
        final String   vendor;
        final String   name;

        SqlScript(Resource resource, Location location, String versionDir, String vendor, String name) {
            this.resource = resource;
            this.location = location;
            this.name = name;
            this.vendor = vendor;
            this.versionDir = versionDir;
        }

        @Override
        public String toString() {
            return "SqlScript{" +
                   "resource=" + resource +
                   ", location=" + location +
                   ", versionDir='" + versionDir + '\'' +
                   ", vendor='" + vendor + '\'' +
                   ", name='" + name + '\'' +
                   '}';
        }
    }
}
