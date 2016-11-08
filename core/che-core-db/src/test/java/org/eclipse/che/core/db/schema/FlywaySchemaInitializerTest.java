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
package org.eclipse.che.core.db.schema;

import org.eclipse.che.core.db.schema.impl.flyway.CustomSqlMigrationResolver;
import org.eclipse.che.core.db.schema.impl.flyway.FlywaySchemaInitializer;
import org.flywaydb.core.api.configuration.FlywayConfiguration;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.RunScript;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Tests {@link FlywaySchemaInitializer}.
 *
 * @author Yevhenii Voevodin
 */
public class FlywaySchemaInitializerTest {

    private FlywaySchemaInitializer initializer;
    private JdbcDataSource          dataSource;

    @BeforeMethod
    public void setUp() {
        dataSource = new JdbcDataSource();
        dataSource.setUrl("jdbc:h2:mem:flyway_test;DB_CLOSE_DELAY=-1");
        initializer = new FlywaySchemaInitializer("h2",
                                                  new String[] {"sql"},
                                                  "",
                                                  ".sql",
                                                  ".",
                                                  dataSource);
    }

    @AfterMethod
    public void cleanUp() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            RunScript.execute(conn, new StringReader("SHUTDOWN"));
        }
    }

    @Test
    public void initializesSchemaWhenDatabaseIsEmpty() throws Exception {
        initializer.init();
    }
}
