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
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.repo.jdbc.JDBCConnectionCapability;

import org.polepos.framework.Car;
import org.polepos.framework.CarMotorFailureException;
import org.polepos.framework.TurnSetup;

import org.polepos.circuits.imola.ImolaDriver;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ImolaCarbonado extends CarbonadoDriver implements ImolaDriver {
    private Storage<StoredPilot> mStorage;
        
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
                        st.execute("DROP TABLE CARBONADO_PILOT");
                    } catch (SQLException e) {
                        // Don't care
                    } finally {
                        st.close();
                    }
                    
                    st = con.createStatement();
                    try {
                        String sql = "CREATE TABLE CARBONADO_PILOT " +
                            "(ID INTEGER NOT NULL" +
                            ",NAME VARCHAR" +
                            ",FIRST_NAME VARCHAR" +
                            ",POINTS INTEGER NOT NULL" +
                            ",LICENSE_ID INTEGER NOT NULL" +
                            ",PRIMARY KEY(ID))";
                        
                        st.execute(sql);
                    } finally {
                        st.close();
                    }
                } finally {
                    cap.yieldConnection(con);
                }
            }
        
            mStorage = repo.storageFor(StoredPilot.class);
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
        
    public void store() {
        try {
            doStore();
        } catch (RepositoryException e) {
            e.printStackTrace();
        }
    }

    public void retrieve() {
        try {
            doRetrieve();
        } catch (RepositoryException e) {
            e.printStackTrace();
        }
    }
    
    private void doStore() throws RepositoryException {
        int objectCount = setup().getObjectCount(); 
        int commitInterval = setup().getCommitInterval();
        int commitCounter = 0;

        Transaction txn = getRepository().enterTransaction(IsolationLevel.READ_UNCOMMITTED);
        try {
            for (int i=1; i<=objectCount; i++) {
                StoredPilot pilot = mStorage.prepare();
                pilot.setID(i);
                pilot.setName("Pilot_" + i);
                pilot.setFirstName("Jonny_" + i);
                pilot.setPoints(i);
                pilot.setLicenseID(i);

                pilot.tryInsert();
                
                if (commitInterval > 0 && ++commitCounter >= commitInterval) {
                    commitCounter = 0;
                    txn.commit();
                }
            }

            txn.commit();
        } finally {
            txn.exit();
        }
    }

    private void doRetrieve() throws RepositoryException {
        int objectCount = setup().getSelectCount();

        for (int i=1; i<=objectCount; i++) {
            StoredPilot pilot = mStorage.prepare();
            pilot.setID(i);
            pilot.load();
            addToCheckSum(pilot.getPoints());
        }
    }
}
