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

import com.amazon.carbonado.Alias;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Index;
import com.amazon.carbonado.Indexes;
import com.amazon.carbonado.Join;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;

import org.polepos.framework.CheckSummable;

/**
 * 
 *
 * @author Brian S O'Neill
 */
@PrimaryKey("ID")
@Indexes({
    @Index("parentID")
})
@Alias("CARBONADO_B4")
public abstract class StoredB4 implements Storable, CheckSummable {
    public abstract int getID();
    public abstract void setID(int id);

    public abstract int getParentID();
    public abstract void setParentID(int id);

    public abstract int getB4();
    public abstract void setB4(int v);

    @Join(internal="parentID", external="ID")
    public abstract StoredB3 getParent() throws FetchException;
    public abstract void setParent(StoredB3 parent);

    public long checkSum() {
        return getB4();
    }

    public void setAll(Repository repo, int id, int value) throws RepositoryException {
        setID(id);
        setB4(value);
        setParentID(id);
        insert();

        StoredB3 b3 = repo.storageFor(StoredB3.class).prepare();
        b3.setID(id);
        b3.setB3(value);
        b3.setParentID(id);
        b3.insert();

        StoredB2 b2 = repo.storageFor(StoredB2.class).prepare();
        b2.setID(id);
        b2.setB2(value);
        b2.setParentID(id);
        b2.insert();

        StoredB1 b1 = repo.storageFor(StoredB1.class).prepare();
        b1.setID(id);
        b1.setB1(value);
        b1.setParentID(id);
        b1.insert();

        StoredB0 b0 = repo.storageFor(StoredB0.class).prepare();
        b0.setID(id);
        b0.setB0(value);
        b0.insert();
    }

    public int getBx(int x) throws FetchException {
        switch (x) {
        case 0:
            return getParent().getParent().getParent().getParent().getB0();
        case 1:
            return getParent().getParent().getParent().getB1();
        case 2:
            return getParent().getParent().getB2();
        case 3:
            return getParent().getB3();
        case 4:
            return getB4();
        }
        throw new IllegalArgumentException();
    }
}
