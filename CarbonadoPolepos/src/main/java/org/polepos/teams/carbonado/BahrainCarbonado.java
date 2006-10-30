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

import com.amazon.carbonado.Cursor;
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

import org.polepos.circuits.bahrain.BahrainDriver;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class BahrainCarbonado extends CarbonadoDriver implements BahrainDriver {
    private Storage<StoredPilotIndexed> mStorage;
        
    public void takeSeatIn(Car car, TurnSetup setup) throws CarMotorFailureException {
        super.takeSeatIn(car, setup);

        Repository repo = getRepository();

        try {
            JDBCConnectionCapability cap = repo.getCapability(JDBCConnectionCapability.class);
            if (cap != null) {
                Connection con = cap.getConnection();
                try {
                    Statement st = con.createStatement();
                    try {
                        st.execute("DROP TABLE CARBONADO_PILOT_INDEXED");
                    } catch (SQLException e) {
                        // Don't care
                    } finally {
                        st.close();
                    }
                    
                    st = con.createStatement();
                    try {
                        String sql = "CREATE TABLE CARBONADO_PILOT_INDEXED " +
                            "(ID INTEGER NOT NULL" +
                            ",NAME VARCHAR" +
                            ",FIRST_NAME VARCHAR" +
                            ",POINTS INTEGER NOT NULL" +
                            ",LICENSE_ID INTEGER NOT NULL" +
                            ",PRIMARY KEY(ID))";
                        
                        st.execute(sql);

                        sql = "CREATE INDEX IX_CARBONADO_PILOT_1 ON " +
                            "CARBONADO_PILOT_INDEXED (NAME)";

                        st.execute(sql);

                        sql = "CREATE INDEX IX_CARBONADO_PILOT_2 ON " +
                            "CARBONADO_PILOT_INDEXED (LICENSE_ID)";

                        st.execute(sql);
                    } finally {
                        st.close();
                    }
                } finally {
                    cap.yieldConnection(con);
                }
            }

            mStorage = repo.storageFor(StoredPilotIndexed.class);
            mStorage.query().deleteAll();
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

    public void query_indexed_string() {
        try {
            doQueryIndexedString();
        } catch (RepositoryException e) {
            e.printStackTrace();
        }
    }
    
    public void query_string() {
        try {
            doQueryString();
        } catch (RepositoryException e) {
            e.printStackTrace();
        }
    }
    
    public void query_indexed_int() {
        try {
            doQueryIndexedInt();
        } catch (RepositoryException e) {
            e.printStackTrace();
        }
    }

    public void query_int(){
        try {
            doQueryInt();
        } catch (RepositoryException e) {
            e.printStackTrace();
        }
    }
        
    public void update() {
        try {
            doUpdate();
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
        int objectCount = setup().getObjectCount(); 
        int commitInterval = setup().getCommitInterval();
        int commitCounter = 0;

        Repository repo = getRepository();

        for (int i=1; i<=objectCount; i++) {
            Transaction txn = repo.enterTransaction(IsolationLevel.READ_UNCOMMITTED);
            try {
                StoredPilotIndexed pilot = mStorage.prepare();
                pilot.setID(i);
                pilot.setName("Pilot_" + i);
                pilot.setFirstName("Jonny_" + i);
                pilot.setPoints(i);
                pilot.setLicenseID(i);

                pilot.insert();
                
                if (commitInterval > 0 && ++commitCounter >= commitInterval) {
                    commitCounter = 0;
                    txn.commit();
                }

                addToCheckSum(i);

                txn.commit();
            } finally {
                txn.exit();
            }
        }
    }

    private void doQueryIndexedString() throws RepositoryException {
        int count = setup().getSelectCount();
        
        Query<StoredPilotIndexed> query = mStorage.query("name = ?");

        for (int i=1; i<=count; i++) {
            Cursor<StoredPilotIndexed> cursor = query.with("Pilot_" + i).fetch();
            while (cursor.hasNext()) {
                StoredPilotIndexed pilot = cursor.next();
                addToCheckSum(pilot.getPoints());
            }
        }
    }

    private void doQueryString() throws RepositoryException {
        int count = setup().getSelectCount();
        
        Query<StoredPilotIndexed> query = mStorage.query("firstName = ?");

        for (int i=1; i<=count; i++) {
            Cursor<StoredPilotIndexed> cursor = query.with("Jonny_" + i).fetch();
            while (cursor.hasNext()) {
                StoredPilotIndexed pilot = cursor.next();
                addToCheckSum(pilot.getPoints());
            }
        }
    }

    private void doQueryIndexedInt() throws RepositoryException {
        int count = setup().getSelectCount();

        Query<StoredPilotIndexed> query = mStorage.query("licenseID = ?");

        for (int i=1; i<=count; i++) {
            Cursor<StoredPilotIndexed> cursor = query.with(i).fetch();
            while (cursor.hasNext()) {
                StoredPilotIndexed pilot = cursor.next();
                addToCheckSum(pilot.getPoints());
            }
        }
    }

    private void doQueryInt() throws RepositoryException {
        int count = setup().getSelectCount();

        Query<StoredPilotIndexed> query = mStorage.query("points = ?");

        for (int i=1; i<=count; i++) {
            Cursor<StoredPilotIndexed> cursor = query.with(i).fetch();
            while (cursor.hasNext()) {
                StoredPilotIndexed pilot = cursor.next();
                addToCheckSum(pilot.getPoints());
            }
        }
    }

    private void doUpdate() throws RepositoryException {
        int count = setup().getUpdateCount();

        Transaction txn = getRepository().enterTransaction(IsolationLevel.READ_UNCOMMITTED);
        try {
            txn.setForUpdate(true);
            Cursor<StoredPilotIndexed> cursor = mStorage.query().fetch();
            try {
                for (int i=1; i<=count && cursor.hasNext(); i++) {
                    StoredPilotIndexed pilot = cursor.next();
                    pilot.setName(pilot.getName().toUpperCase());
                    pilot.update();
                    addToCheckSum(1);
                }
            } finally {
                cursor.close();
            }
            txn.commit();
        } finally {
            txn.exit();
        }
    }

    /**
     * deleting one at a time, simulating deleting individual objects  
     */
    private void doDelete() throws RepositoryException {
        int count = setup().getObjectCount();
        
        for (int i=1; i<=count; i++) {
            StoredPilotIndexed pilot = mStorage.prepare();
            pilot.setID(i);
            pilot.delete();
            addToCheckSum(1);
        }
    }
}
