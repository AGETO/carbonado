/*
 * Copyright 2006 Amazon Technologies, Inc. or its affiliates.
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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Transaction;

/**
 *
 *
 * @author Brian S O'Neill
 */
class OracleSupportStrategy extends JDBCSupportStrategy {
    private static final int LOB_CHUNK_LIMIT = 4000;

    private static final String PLAN_TABLE_NAME = "TEMP_CARBONADO_PLAN_TABLE";

    protected OracleSupportStrategy(JDBCRepository repo) {
        super(repo);
    }

    JDBCExceptionTransformer createExceptionTransformer() {
        return new OracleExceptionTransformer();
    }

    String createSequenceQuery(String sequenceName) {
        return new StringBuilder(25 + sequenceName.length())
            .append("SELECT ").append(sequenceName).append(".NEXTVAL FROM DUAL")
            .toString();
    }

    JDBCBlob convertBlob(java.sql.Blob blob, JDBCBlobLoader loader) {
        if (blob instanceof oracle.sql.BLOB) {
            return new OracleBlob(mRepo, (oracle.sql.BLOB) blob, loader);
        }
        return super.convertBlob(blob, loader);
    }

    JDBCClob convertClob(java.sql.Clob clob, JDBCClobLoader loader) {
        if (clob instanceof oracle.sql.CLOB) {
            return new OracleClob(mRepo, (oracle.sql.CLOB) clob, loader);
        }
        return super.convertClob(clob, loader);
    }

    /**
     * @return original blob if too large and post-insert update is required, null otherwise
     * @throws PersistException instead of FetchException since this code is
     * called during an insert operation
     */
    com.amazon.carbonado.lob.Blob setBlobValue(PreparedStatement ps, int column,
                                               com.amazon.carbonado.lob.Blob blob)
        throws PersistException
    {
        try {
            long length = blob.getLength();
            if (length > LOB_CHUNK_LIMIT || ((long) ((int) length)) != length) {
                ps.setBlob(column, oracle.sql.BLOB.empty_lob());
                return blob;
            }

            if (blob instanceof OracleBlob) {
                ps.setBlob(column, ((OracleBlob) blob).getInternalBlobForPersist());
                return null;
            }

            ps.setBinaryStream(column, blob.openInputStream(), (int) length);
            return null;
        } catch (SQLException e) {
            throw mRepo.toPersistException(e);
        } catch (FetchException e) {
            throw e.toPersistException();
        }
    }

    /**
     * @return original clob if too large and post-insert update is required, null otherwise
     */
    com.amazon.carbonado.lob.Clob setClobValue(PreparedStatement ps, int column,
                                               com.amazon.carbonado.lob.Clob clob)
        throws PersistException
    {
        try {
            long length = clob.getLength();
            if (length > LOB_CHUNK_LIMIT || ((long) ((int) length)) != length) {
                ps.setClob(column, oracle.sql.CLOB.empty_lob());
                return clob;
            }

            if (clob instanceof OracleClob) {
                ps.setClob(column, ((OracleClob) clob).getInternalClobForPersist());
                return null;
            }

            ps.setCharacterStream(column, clob.openReader(), (int) length);
            return null;
        } catch (SQLException e) {
            throw mRepo.toPersistException(e);
        } catch (FetchException e) {
            throw e.toPersistException();
        }
    }

    /* FIXME
    boolean printPlan(Appendable app, int indentLevel, String statement)
        throws FetchException, IOException
    {
        Transaction txn = mRepo.enterTransaction();
        try {
            Connection con = mRepo.getConnection();
            try {
                try {
                    return printPlan(app, indentLevel, statement, con);
                } catch (SQLException e) {
                    throw mRepo.toFetchException(e);
                }
            } finally {
                mRepo.yieldConnection(con);
            }
        } finally {
            try {
                txn.exit();
            } catch (PersistException e) {
                // I don't care.
            }
        }
    }

    private boolean printPlan(Appendable app, int indentLevel, String statement, Connection con)
        throws SQLException, IOException
    {
        preparePlanTable(con);

        String explainPlanStatement =
            "EXPLAIN PLAN INTO " + PLAN_TABLE_NAME + " FOR " +
            statement;

        Statement st = con.createStatement();
        try {
            st.execute(explainPlanStatement);
        } finally {
            st.close();
        }

        st = con.createStatement();
        try {
            String planStatement =
                "SELECT LEVEL, OPERATION, OPTIONS, OBJECT_NAME, CARDINALITY, BYTES, COST " +
                "FROM " + PLAN_TABLE_NAME + " " +
                "START WITH ID=0 " +
                "CONNECT BY PRIOR ID = PARENT_ID " +
                "AND PRIOR NVL(STATEMENT_ID, ' ') = NVL(STATEMENT_ID, ' ') " +
                "AND PRIOR TIMESTAMP <= TIMESTAMP " +
                "ORDER BY ID, POSITION";

            ResultSet rs = st.executeQuery(planStatement);
            try {
                while (rs.next()) {
                    BaseQuery.indent(app, indentLevel + (rs.getInt(1) - 1) * 2);

                    app.append(rs.getString(2));
                    String options = rs.getString(3);
                    if (options != null && options.length() > 0) {
                        app.append(" (");
                        app.append(options);
                        app.append(')');
                    }

                    String name = rs.getString(4);
                    if (name != null && name.length() > 0) {
                        app.append(' ');
                        app.append(name);
                    }

                    app.append(" {");

                    String[] extraNames = {
                        "rows", "CARDINALITY",
                        "bytes", "BYTES",
                        "cost", "COST",
                    };

                    boolean comma = false;
                    for (int i=0; i<extraNames.length; i+=2) {
                        String str = rs.getString(extraNames[i + 1]);
                        if (str != null && str.length() > 0) {
                            if (comma) {
                                app.append(", ");
                            }
                            app.append(extraNames[i]);
                            app.append('=');
                            app.append(str);
                            comma = true;
                        }
                    }

                    app.append('}');
                    app.append('\n');
                }
            } finally {
                rs.close();
            }
        } finally {
            st.close();
        }

        return true;
    }

    private void preparePlanTable(Connection con) throws SQLException {
        Statement st = con.createStatement();
        try {
            // TODO: Is there a better way to check if a table exists?
            st.execute("SELECT COUNT(*) FROM " + PLAN_TABLE_NAME);
            return;
        } catch (SQLException e) {
            // Assume table doesn't exist, so create it.
        } finally {
            st.close();
        }

        String statement =
            "CREATE GLOBAL TEMPORARY TABLE " + PLAN_TABLE_NAME + " (" +
            "STATEMENT_ID VARCHAR2(30)," +
            "TIMESTAMP DATE," +
            "REMARKS VARCHAR2(80)," +
            "OPERATION VARCHAR2(30)," +
            "OPTIONS VARCHAR2(30)," +
            "OBJECT_NODE VARCHAR2(128)," +
            "OBJECT_OWNER VARCHAR2(30)," +
            "OBJECT_NAME VARCHAR2(30)," +
            "OBJECT_INSTANCE NUMBER(38)," +
            "OBJECT_TYPE VARCHAR2(30)," +
            "OPTIMIZER VARCHAR2(255)," +
            "SEARCH_COLUMNS NUMBER," +
            "ID NUMBER(38)," +
            "PARENT_ID NUMBER(38)," +
            "POSITION NUMBER(38)," +
            "COST NUMBER(38)," +
            "CARDINALITY NUMBER(38)," +
            "BYTES NUMBER(38)," +
            "OTHER_TAG VARCHAR2(255)," +
            "PARTITION_START VARCHAR2(255)," +
            "PARTITION_STOP VARCHAR2(255)," +
            "PARTITION_ID NUMBER(38),"+
            "OTHER LONG," +
            "DISTRIBUTION VARCHAR2(30)" +
            ")";

        st = con.createStatement();
        try {
            st.execute(statement);
        } finally {
            st.close();
        }
    }
    */
}
