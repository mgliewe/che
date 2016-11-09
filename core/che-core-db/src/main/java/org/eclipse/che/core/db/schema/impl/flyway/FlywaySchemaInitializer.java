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

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.eclipse.che.core.db.schema.SchemaInitializationException;
import org.eclipse.che.core.db.schema.SchemaInitializer;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;

/**
 * <a href="https://flywaydb.org/">Flyway</a> based schema initializer.
 *
 * @author Yevhenii Voevodin
 */
public class FlywaySchemaInitializer implements SchemaInitializer {

    private final DataSource dataSource;
    private final String     providerName;
    private final String[]   locations;
    private final String     scriptsPrefix;
    private final String     scriptsSuffix;
    private final String     versionSeparator;
    private final boolean    baselineOnMigrate;
    private final String     baselineVersion;

    @Inject
    public FlywaySchemaInitializer(@Named("db.provider.name") String providerName,
                                   @Named("db.schema.flyway.scripts.locations") String[] scriptsLocations,
                                   @Named("db.schema.flyway.scripts.prefix") String scriptsPrefix,
                                   @Named("db.schema.flyway.scripts.suffix") String scriptsSuffix,
                                   @Named("db.schema.flyway.version.separator") String versionSeparator,
                                   @Named("db.schema.flyway.baseline_on_migrate") boolean baselineOnMigrate,
                                   @Named("db.schema.flyway.baseline_version") String baselineVersion,
                                   DataSource dataSource) {
        this.dataSource = dataSource;
        this.locations = scriptsLocations;
        this.providerName = providerName;
        this.scriptsPrefix = scriptsPrefix;
        this.scriptsSuffix = scriptsSuffix;
        this.versionSeparator = versionSeparator;
        this.baselineOnMigrate = baselineOnMigrate;
        this.baselineVersion = baselineVersion;
    }

    @Override
    public void init() throws SchemaInitializationException {
        final Flyway flyway = new Flyway();
        flyway.setDataSource(dataSource);
        flyway.setLocations(locations);
        flyway.setClassLoader(Thread.currentThread().getContextClassLoader());
        flyway.setResolvers(new CustomSqlMigrationResolver(providerName));
        flyway.setSkipDefaultResolvers(true);
        flyway.setBaselineOnMigrate(baselineOnMigrate);
        flyway.setBaselineVersionAsString(baselineVersion);
        flyway.setSqlMigrationSeparator(versionSeparator);
        flyway.setSqlMigrationSuffix(scriptsSuffix);
        flyway.setSqlMigrationPrefix(scriptsPrefix);
        try {
            flyway.migrate();
        } catch (RuntimeException x) {
            throw new SchemaInitializationException(x.getLocalizedMessage(), x);
        }
    }
}
