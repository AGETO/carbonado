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

import java.util.ArrayList;
import java.util.List;

import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.repo.jdbc.JDBCRepositoryBuilder;
import com.amazon.carbonado.repo.sleepycat.BDBRepositoryBuilder;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import org.polepos.framework.Car;
import org.polepos.framework.CarMotorFailureException;
import org.polepos.framework.Driver;
import org.polepos.framework.Team;

import org.polepos.teams.jdbc.Jdbc;

public class CarbonadoTeam extends Team {

    private final Car[] mCars;
    private final CarbonadoDriver[] mDrivers;
        
    public CarbonadoTeam() {
        List<Repository> repos = new ArrayList<Repository>();

        {
            BDBRepositoryBuilder builder = new BDBRepositoryBuilder();
            builder.setName("BDBJE");
            builder.setProduct("JE");
            builder.setEnvironmentHome("data/Carbonado-BDBJE");
            builder.setTransactionNoSync(true);
            try {
                repos.add(builder.build());
            } catch (RepositoryException e) {
                e.printStackTrace();
            }
        }

        {
            BDBRepositoryBuilder builder = new BDBRepositoryBuilder();
            builder.setName("BDB");
            builder.setProduct("DB");
            builder.setEnvironmentHome("data/Carbonado-BDB");
            builder.setTransactionNoSync(true);
            try {
                repos.add(builder.build());
            } catch (RepositoryException e) {
                e.printStackTrace();
            }
        }

        {
            // No set of Carbonado types, so steal Hibernate types.
            String[] dbs = Jdbc.settings().getHibernateTypes();
            for (String db : dbs) {
                JDBCRepositoryBuilder builder = new JDBCRepositoryBuilder();
                builder.setName(db);

                ComboPooledDataSource ds = new ComboPooledDataSource();
                try {
                    ds.setDriverClass(Jdbc.settings().getDriverClass(db));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                ds.setJdbcUrl(Jdbc.settings().getConnectUrl(db));
                ds.setUser(Jdbc.settings().getUsername(db));
                ds.setPassword(Jdbc.settings().getPassword(db));

                ds.setMaxStatements(100);

                builder.setDataSource(ds);

                try {
                    repos.add(builder.build());
                } catch (RepositoryException e) {
                    e.printStackTrace();
                }
            }
        }

        mCars = new Car[repos.size()];
        for (int i=0; i<repos.size(); i++) {
            mCars[i] = new CarbonadoCar(repos.get(i));
        }

        mDrivers = new CarbonadoDriver[] {
            new MelbourneCarbonado(),
            new SepangCarbonado(),
            new BahrainCarbonado(),
            new ImolaCarbonado(),
            new BarcelonaCarbonado()
        };
    }
        
    @Override
    public String name() {
        return "Carbonado";
    }

    @Override
    public String description() {
        return "Tests Carbonado against various repositories";
    }

    @Override
    public Car[] cars() {
        return mCars;
    }

    @Override
    public Driver[] drivers() {
        return mDrivers;
    }

    @Override
    public String website() {
        return null;
    }

}
