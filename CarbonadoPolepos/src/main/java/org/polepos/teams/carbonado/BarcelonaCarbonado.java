/* 
This file is part of the PolePosition database benchmark
http://www.polepos.org

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public
License along with this program; if not, write to the Free
Software Foundation, Inc., 59 Temple Place - Suite 330, Boston,
MA  02111-1307, USA. */

package org.polepos.teams.carbonado;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.repo.jdbc.JDBCConnectionCapability;

import org.polepos.framework.Car;
import org.polepos.framework.CarMotorFailureException;
import org.polepos.framework.TurnSetup;

import org.polepos.circuits.barcelona.BarcelonaDriver;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class BarcelonaCarbonado extends CarbonadoDriver implements BarcelonaDriver {
    private Storage<StoredB4> mStorageB4;
    
    public void takeSeatIn(Car car, TurnSetup setup) throws CarMotorFailureException{
        super.takeSeatIn(car, setup);
                
        Repository repo = getRepository();

        try {
            JDBCConnectionCapability cap = repo.getCapability(JDBCConnectionCapability.class);
            if (cap != null) {
                Connection con = cap.getConnection();
                try {
                    for (int i=0; i<=4; i++) {
                        Statement st = con.createStatement();
                        try {
                            st.execute("DROP TABLE CARBONADO_B" + i);
                        } catch (SQLException e) {
                            // Don't care
                        } finally {
                            st.close();
                        }
                    }

                    for (int i=0; i<=4; i++) {
                        Statement st = con.createStatement();
                        try {
                            String sql = "CREATE TABLE CARBONADO_B" + i + ' ' +
                                "(ID INTEGER NOT NULL" +
                                ",B" + i + " INTEGER NOT NULL";

                            if (i != 0) {
                                sql += ",PARENT_ID INTEGER NOT NULL";
                            }

                            sql += ",PRIMARY KEY(ID))";
                        
                            st.execute(sql);

                            if (i != 0) {
                                sql = "CREATE INDEX IX_CARBONADO_B" + i + "_PARENT ON " +
                                    "CARBONADO_B" + i + " (PARENT_ID)";

                                st.execute(sql);
                            }

                            if (i == 2) {
                                sql = "CREATE INDEX IX_CARBONADO_B" + i + " ON " +
                                    "CARBONADO_B" + i + " (B" + i + ")";

                                st.execute(sql);
                            }
                        } finally {
                            st.close();
                        }
                    }
                } finally {
                    cap.yieldConnection(con);
                }
            }

            mStorageB4 = repo.storageFor(StoredB4.class);
            mStorageB4.query().deleteAll();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new CarMotorFailureException();
        } catch (RepositoryException e) {
            e.printStackTrace();
            throw new CarMotorFailureException();
        }
    }
        
    public void backToPit() {
        super.backToPit();
    }

    public void write() {
        try {
            doWrite();
        } catch (RepositoryException e) {
            e.printStackTrace();
        }
    }

    public void read() {
        try {
            doRead();
        } catch (RepositoryException e) {
            e.printStackTrace();
        }
    }
    
    public void query() {
        try {
            doQuery();
        } catch (RepositoryException e) {
            e.printStackTrace();
        }
    }
    
    public void delete() {
        try {
            doDelete();
        } catch (RepositoryException e) {
            e.printStackTrace();
        }
    }

    private void doWrite() throws RepositoryException {
        int count = setup().getObjectCount();

        Repository repo = getRepository();

        for (int i=1; i<=count; i++) {
            Transaction txn = repo.enterTransaction(IsolationLevel.READ_UNCOMMITTED);
            try {
                StoredB4 b4 = mStorageB4.prepare();
                b4.setAll(repo, i, i);
                txn.commit();
            } finally {
                txn.exit();
            }
        }
    }
    
    private void doRead() throws RepositoryException {
        int count = setup().getObjectCount();

        for (int i=1; i<=count; i++) {
            StoredB4 b4 = mStorageB4.prepare();
            b4.setID(i);
            b4.load();
            // Force load of all parents.
            b4.getBx(0);

            addToCheckSum(b4.checkSum());
        }
    }
    
    private void doQuery() throws RepositoryException {
        int count = setup().getSelectCount();

        Query<StoredB4> query = mStorageB4.query("parent.parent.b2 = ?");

        for (int i=1; i<=count; i++) {
            StoredB4 b4 = query.with(i).loadOne();

            // Force load of all parents.
            StoredB3 b3 = b4.getParent();
            StoredB2 b2 = b3.getParent();
            StoredB1 b1 = b2.getParent();
            StoredB0 b0 = b1.getParent();

            addToCheckSum(b4.checkSum());
        }
    }

    private void doDelete() throws RepositoryException {
        int count = setup().getObjectCount();

        Repository repo = getRepository();

        for (int i=1; i<=count; i++) {
            Transaction txn = repo.enterTransaction(IsolationLevel.READ_UNCOMMITTED);
            txn.setForUpdate(true);
            try {
                StoredB4 b4 = mStorageB4.prepare();
                b4.setID(i);
                b4.load();
                b4.delete();

                StoredB3 b3 = b4.getParent();
                b3.delete();

                StoredB2 b2 = b3.getParent();
                b2.delete();

                StoredB1 b1 = b2.getParent();
                b1.delete();

                StoredB0 b0 = b1.getParent();
                b0.delete();

                addToCheckSum(5);
                txn.commit();
            } finally {
                txn.exit();
            }
        }
    }
}
