/*
 * Copyright 2008-2010 Amazon Technologies, Inc. or its affiliates.
 * Amazon, Amazon.com and Carbonado are trademarks or registered trademarks
 * of Amazon Technologies, Inc. or its affiliates.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazon.carbonado.repo.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.ArrayList;
import java.util.List;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.joda.time.DateTime;

import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryBuilder;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.lob.Blob;
import com.amazon.carbonado.lob.Clob;

import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableKey;
import com.amazon.carbonado.info.StorableProperty;

/**
 * Creates tables as needed automatically for the H2 database.
 *
 * @author Brian S O'Neill
 */
public class H2SchemaResolver implements SchemaResolver {
    public <S extends Storable> boolean resolve(StorableInfo<S> info,
                                                Connection con, String catalog, String schema)
        throws SQLException
    {
        // Create the table, keys, indexes and sequences.
        List<String> sequenceNames = new ArrayList<String>();
        
        StringBuilder b = new StringBuilder();
        b.append("CREATE TABLE ");

        String tableName;
        if (info.getAliasCount() > 0) {
            tableName = info.getAlias(0);
        } else {
            tableName = info.getName();
        }
        if (needsQuotes(tableName)) {
            tableName = '"' + tableName + '"';
        }
        b.append(tableName);
        b.append(" (");

        int i = 0;
        for (StorableProperty<S> property : info.getAllProperties().values()) {
            if (property.isJoin() || property.isDerived()) {
                continue;
            }

            if (i++ > 0) {
                b.append(", ");
            }

            if (property.getAliasCount() > 0) {
                b.append(property.getAlias(0));
            } else {
                b.append(property.getName());
            }
            b.append(' ');

            Class type = property.getType();
            String typeName;
            if (type == String.class) {
                typeName = "VARCHAR";
            } else if (type == int.class || type == Integer.class) {
                typeName = "INT";
            } else if (type == long.class || type == Long.class) {
                typeName = "BIGINT";
            } else if (type == float.class || type == Float.class) {
                typeName = "REAL";
            } else if (type == double.class || type == Double.class) {
                typeName = "DOUBLE";
            } else if (type == short.class || type == Short.class) {
                typeName = "SMALLINT";
            } else if (type == byte.class || type == Byte.class) {
                typeName = "TINYINT";
            } else if (type == char.class || type == Character.class) {
                typeName = "CHAR(1)";
            } else if (type == boolean.class || type == Boolean.class) {
                typeName = "BOOLEAN";
            } else if (type == DateTime.class) {
                typeName = "TIMESTAMP";
            } else if (type == byte[].class) {
                typeName = "BINARY";
            } else if (type == Blob.class) {
                typeName = "BLOB";
            } else if (type == Clob.class) {
                typeName = "CLOB";
            } else if (type == BigDecimal.class || type == BigInteger.class) {
                typeName = "NUMBER";
            } else if (type == Object.class) {
                // Object type is used by some tests.
                typeName = "VARCHAR";
            } else {
                return false;
            }

            b.append(typeName);

            if (property.isNullable()) {
                b.append(" NULL");
            } else {
                b.append(" NOT NULL");
            }

            if (property.getSequenceName() != null) {
                sequenceNames.add(property.getSequenceName());
            }
        }

        b.append(')');
        String createTable = b.toString();

        b = new StringBuilder();
        b.append("ALTER TABLE ");
        b.append(tableName);
        b.append(" ADD PRIMARY KEY (");

        i = 0;
        for (StorableProperty<S> property : info.getPrimaryKeyProperties().values()) {
            if (i++ > 0) {
                b.append(", ");
            }

            if (property.getAliasCount() > 0) {
                b.append(property.getAlias(0));
            } else {
                b.append(property.getName());
            }
        }

        b.append(')');
        String createPrimaryKey = b.toString();

        int indexCount = 0;

        List<String> createAltKeys = new ArrayList<String>();

        for (StorableKey<S> key : info.getAlternateKeys()) {
            b = new StringBuilder();
            b.append("CREATE UNIQUE INDEX ");
            b.append(tableName);
            b.append('_');
            b.append(Integer.valueOf(++indexCount));
            b.append(" ON ");
            b.append(tableName);
            b.append(" (");

            i = 0;
            for (OrderedProperty<S> op : key.getProperties()) {
                StorableProperty<S> property = op.getChainedProperty().getPrimeProperty();
                if (property.isDerived()) {
                    continue;
                }

                if (i++ > 0) {
                    b.append(", ");
                }

                if (property.getAliasCount() > 0) {
                    b.append(property.getAlias(0));
                } else {
                    b.append(property.getName());
                }
            }

            if (i > 0) {
                b.append(')');
                createAltKeys.add(b.toString());
            }
        }

        List<String> createIndexes = new ArrayList<String>();

        for (StorableIndex<S> index : info.getIndexes()) {
            b = new StringBuilder();
            b.append("CREATE INDEX ");
            b.append(tableName);
            b.append('_');
            b.append(Integer.valueOf(++indexCount));
            b.append(" ON ");
            b.append(tableName);
            b.append(" (");

            i = 0;
            for (StorableProperty<S> property : index.getProperties()) {
                if (property.isDerived()) {
                    continue;
                }

                if (i++ > 0) {
                    b.append(", ");
                }

                if (property.getAliasCount() > 0) {
                    b.append(property.getAlias(0));
                } else {
                    b.append(property.getName());
                }
            }

            if (i > 0) {
                b.append(')');
                createIndexes.add(b.toString());
            }
        }

        Statement st = con.createStatement();
        try {
            st.executeUpdate(createTable);

            st.executeUpdate(createPrimaryKey);

            for (String createAltKey : createAltKeys) {
                st.executeUpdate(createAltKey);
            }

            for (String createIndex : createIndexes) {
                st.executeUpdate(createIndex);
            }

            for (String sequenceName : sequenceNames) {
                st.executeUpdate("CREATE SEQUENCE IF NOT EXISTS " + sequenceName);
            }
        } finally {
            st.close();
        }

        return true;
    }

    private boolean needsQuotes(String str) {
        if (!Character.isUnicodeIdentifierStart(str.charAt(0))) {
            return true;
        }
        for (int i=1; i<str.length(); i++) {
            if (!Character.isUnicodeIdentifierPart(str.charAt(i))) {
                return true;
            }
        }
        return false;
    }
}
