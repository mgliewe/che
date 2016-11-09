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

import com.google.common.collect.ListMultimap;
import com.google.common.primitives.Ints;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.configuration.FlywayConfiguration;

import java.util.TreeMap;
import java.util.regex.Pattern;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;

/**
 * Indexes scripts according to the version defined by script names.
 * For example if 2 scripts are under the same version directory e.g. <i>5.0.0</i>
 * with names <i>1.init.sql</i> and <i>100.migrate_something.sql</i>
 * will be indexed as <i>1 -> 1.init.sql</i> and <i>2 -> 100.migrate_something.sql</i>,
 * with database versions 5.0.0.1 and 5.0.0.2 respectively.
 *
 * @author Yevhenii Voevodin
 */
class ScriptsIndexer {

    private static final Pattern NOT_VERSION_CHARS_PATTERN = Pattern.compile("[^0-9.]");

    /**
     * Modifies scripts by setting their db migration version and index.
     *
     * @param versionToScripts
     *         map of script where key is {@link SqlScript#versionDir}
     *         and value is list of script with such version
     * @param configuration
     *         flyway configuration used for detecting script index
     */
    void indexScripts(ListMultimap<String, SqlScript> versionToScripts, FlywayConfiguration configuration) {
        for (String versionDir : versionToScripts.keySet()) {
            // 5.0.0-M1 -> 5.0.0.M1
            final String noDashesVersion = versionDir.replace("-", ".");
            // 5.0.0.M1 -> 5.0.0.1
            final String normalizedDirVersion = NOT_VERSION_CHARS_PATTERN.matcher(noDashesVersion).replaceAll("");

            // extract versions from script names and put them in sorted map
            final TreeMap<Integer, SqlScript> scriptsOrder = new TreeMap<>();
            for (SqlScript sqlScript : versionToScripts.get(versionDir)) {
                // separate version from the other part of the name
                final int sepIdx = sqlScript.name.indexOf(configuration.getSqlMigrationSeparator());
                if (sepIdx == -1) {
                    throw new FlywayException(format("sql script name '%s' is not valid, name must contain '%s'",
                                                     sqlScript.name,
                                                     configuration.getSqlMigrationSeparator()));
                }

                // check whether part before separator is not empty
                String rawVersion = sqlScript.name.substring(0, sepIdx);
                if (rawVersion.isEmpty()) {
                    throw new FlywayException(format("sql script name '%s' is not valid, name must provide version like " +
                                                     "'4%smigration_description.sql",
                                                     sqlScript.name,
                                                     configuration.getSqlMigrationSeparator()));
                }

                // extract sql script version without prefix
                final String prefix = configuration.getSqlMigrationPrefix();
                if (!isNullOrEmpty(prefix) && sqlScript.name.startsWith(prefix)) {
                    rawVersion = rawVersion.substring(prefix.length());
                }
                final Integer intVersion = Ints.tryParse(rawVersion);
                if (intVersion == null) {
                    throw new FlywayException(format("version provided by sql script name '%s' must be an integer",
                                                     rawVersion));
                }
                sqlScript.version = intVersion;
                final SqlScript oldScript = scriptsOrder.put(intVersion, sqlScript);
                if (oldScript != null) {
                    throw new FlywayException(format("Two scripts with the same version '%d', dumping scripts:\n" +
                                                     "First: %s\n, " +
                                                     "Second: %s",
                                                     intVersion,
                                                     sqlScript,
                                                     oldScript));
                }
            }

            // index scripts
            int idx = 0;
            for (SqlScript sqlScript : scriptsOrder.values()) {
                sqlScript.index = ++idx;
                sqlScript.migrationVersion = MigrationVersion.fromVersion(normalizedDirVersion + '.' + idx);
            }
        }
    }
}
