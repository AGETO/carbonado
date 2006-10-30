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
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Storable;

/**
 * 
 *
 * @author Brian S O'Neill
 */
@PrimaryKey("ID")
@Indexes({
    @Index("parentID")
})
@Alias("CARBONADO_B1")
public interface StoredB1 extends Storable {
    int getID();
    void setID(int id);

    int getParentID();
    void setParentID(int id);

    int getB1();
    void setB1(int v);

    @Join(internal="parentID", external="ID")
    StoredB0 getParent() throws FetchException;
    void setParent(StoredB0 parent);

    @Join(internal="ID", external="parentID")
    Query<StoredB2> getChildren() throws FetchException;
}
