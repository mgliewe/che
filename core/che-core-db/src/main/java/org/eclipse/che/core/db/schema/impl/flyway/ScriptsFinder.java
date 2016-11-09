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

import org.flywaydb.core.api.configuration.FlywayConfiguration;
import org.flywaydb.core.internal.util.Location;
import org.flywaydb.core.internal.util.scanner.Resource;
import org.flywaydb.core.internal.util.scanner.classpath.ClassPathScanner;
import org.flywaydb.core.internal.util.scanner.filesystem.FileSystemScanner;

import java.io.File;
import java.io.IOException;

import static java.lang.String.format;

/**
 * Searches for sql scripts in given places.
 *
 * @author Yevhenii Voevodin
 */
class ScriptsFinder {

    /**
     * Finds scripts in configured {@link FlywayConfiguration#getLocations()}.
     *
     * @param configuration
     *         flyway configuration to find scripts
     * @return found scripts as script name to list of sql scripts with such name
     * @throws IOException
     *         when any io error occurs during scripts look up
     */
    ListMultimap<String, SqlScript> findScripts(FlywayConfiguration configuration) throws IOException {
        final ClassPathScanner cpScanner = new ClassPathScanner(configuration.getClassLoader());
        final FileSystemScanner fsScanner = new FileSystemScanner();
        final ListMultimap<String, SqlScript> scripts = ArrayListMultimap.create();
        for (String rawLocation : configuration.getLocations()) {
            final Location location = new Location(rawLocation);
            final Resource[] resources;
            if (location.isClassPath()) {
                resources = cpScanner.scanForResources(location,
                                                       configuration.getSqlMigrationPrefix(),
                                                       configuration.getSqlMigrationSuffix());
            } else {
                resources = fsScanner.scanForResources(location,
                                                       configuration.getSqlMigrationPrefix(),
                                                       configuration.getSqlMigrationSuffix());
            }
            for (Resource resource : resources) {
                final SqlScript script = createScript(resource, location);
                scripts.put(script.name, script);
            }
        }
        return scripts;
    }

    private static SqlScript createScript(Resource resource, Location location) {
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
}
