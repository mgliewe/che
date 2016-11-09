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
import com.google.common.hash.Hashing;
import com.google.common.io.ByteSource;

import org.flywaydb.core.api.MigrationType;
import org.flywaydb.core.api.resolver.BaseMigrationResolver;
import org.flywaydb.core.api.resolver.ResolvedMigration;
import org.flywaydb.core.internal.dbsupport.DbSupportFactory;
import org.flywaydb.core.internal.resolver.ResolvedMigrationImpl;
import org.flywaydb.core.internal.resolver.sql.SqlMigrationExecutor;
import org.flywaydb.core.internal.util.PlaceholderReplacer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private static final Logger LOG                 = LoggerFactory.getLogger(CustomSqlMigrationResolver.class);
    private static final String DEFAULT_VENDOR_NAME = "default";

    private final String         vendorName;
    private final ScriptsFinder  finder;
    private final ScriptsIndexer indexer;

    public CustomSqlMigrationResolver(String dbProviderName) {
        this.vendorName = dbProviderName;
        this.finder = new ScriptsFinder();
        this.indexer = new ScriptsIndexer();
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
        final ListMultimap<String, SqlScript> scripts = finder.findScripts(flywayConfiguration);
        LOG.debug("Found scripts: {}", scripts);

        // filter sql scripts according to current db provider
        final ListMultimap<String, SqlScript> versionToScripts = ArrayListMultimap.create();
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
                versionToScripts.put(pickedScript.versionDir, pickedScript);
            }
        }

        indexer.indexScripts(versionToScripts, flywayConfiguration);

        final List<ResolvedMigration> migrations = new ArrayList<>(versionToScripts.size());
        for (SqlScript script : versionToScripts.values()) {
            final ResolvedMigrationImpl migration = new ResolvedMigrationImpl();
            migration.setVersion(script.migrationVersion);
            if (script.location.isFileSystem()) {
                migration.setPhysicalLocation(script.resource.getLocationOnDisk());
                migration.setScript(migration.getPhysicalLocation());
            } else {
                migration.setScript(script.resource.getLocation());
            }
            migration.setType(MigrationType.SQL);
            // TODO extract description between prefix + separator and suffix
            migration.setDescription(script.name);
            migration.setChecksum(ByteSource.wrap(script.resource.loadAsBytes()).hash(Hashing.crc32()).asInt());
            final Connection connection = flywayConfiguration.getDataSource().getConnection();
            // TODO configure placeholders + connection
            migration.setExecutor(new SqlMigrationExecutor(DbSupportFactory.createDbSupport(connection, true),
                                                           script.resource,
                                                           PlaceholderReplacer.NO_PLACEHOLDERS,
                                                           flywayConfiguration.getEncoding()));
            migrations.add(migration);
        }
        return migrations;
    }
}
