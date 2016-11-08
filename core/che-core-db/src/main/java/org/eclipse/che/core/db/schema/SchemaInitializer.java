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

import com.google.inject.ImplementedBy;

import org.eclipse.che.core.db.schema.impl.flyway.FlywaySchemaInitializer;

/**
 * Initializes database schema or migrates an old version of it to a new one.
 *
 * @author Yevhenii Voevodin
 */
@ImplementedBy(FlywaySchemaInitializer.class)
public interface SchemaInitializer {

    /**
     * Initializes database schema or migrates an old schema to a new one.
     *
     * @throws SchemaInitializationException
     *         thrown when any error occurs during schema initialization/migration
     */
    void init() throws SchemaInitializationException;
}
