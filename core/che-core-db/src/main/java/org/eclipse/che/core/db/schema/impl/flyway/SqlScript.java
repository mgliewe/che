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

import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.internal.util.Location;
import org.flywaydb.core.internal.util.scanner.Resource;

import java.util.Objects;

/**
 * Defines a data structure for holding information about sql script.
 *
 * @author Yevhenii Voevodin
 */
class SqlScript {

    final Resource resource;
    final Location location;
    final String   versionDir;
    final String   vendor;
    final String   name;

    int              version;
    int              index;
    MigrationVersion migrationVersion;

    SqlScript(Resource resource, Location location, String versionDir, String vendor, String name) {
        this.resource = resource;
        this.location = location;
        this.name = name;
        this.vendor = vendor;
        this.versionDir = versionDir;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SqlScript)) {
            return false;
        }
        final SqlScript that = (SqlScript)obj;
        return index == that.index
               && Objects.equals(resource.getLocation(), that.resource.getLocation())
               && Objects.equals(location, that.location)
               && Objects.equals(versionDir, that.versionDir)
               && Objects.equals(vendor, that.vendor)
               && Objects.equals(name, that.name)
               && Objects.equals(version, version);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + Objects.hashCode(resource.getLocation());
        hash = 31 * hash + Objects.hashCode(location);
        hash = 31 * hash + Objects.hashCode(versionDir);
        hash = 31 * hash + Objects.hashCode(vendor);
        hash = 31 * hash + Objects.hashCode(name);
        hash = 31 * hash + index;
        hash = 31 * hash + Objects.hashCode(version);
        return hash;
    }

    @Override
    public String toString() {
        return "SqlScript{" +
               "resource=" + resource +
               ", location=" + location +
               ", versionDir='" + versionDir + '\'' +
               ", vendor='" + vendor + '\'' +
               ", name='" + name + '\'' +
               ", index=" + index +
               ", migrationVersion=" + migrationVersion +
               '}';
    }
}
