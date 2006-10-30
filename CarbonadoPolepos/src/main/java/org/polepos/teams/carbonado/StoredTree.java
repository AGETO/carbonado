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
import com.amazon.carbonado.Join;
import com.amazon.carbonado.Nullable;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;

/**
 * 
 *
 * @author Brian S O'Neill
 */
@PrimaryKey("ID")
@Alias("CARBONADO_TREE")
public abstract class StoredTree implements Storable {

    private static int cNextID;

    public static StoredTree createTree(Storage<StoredTree> storage, int depth)
        throws PersistException
    {
        cNextID = 0;
        return createTree(storage, depth, 0);
    }
    
    private static StoredTree createTree(Storage<StoredTree> storage,
                                         int maxDepth, int currentDepth)
        throws PersistException
    {
        if (maxDepth <= 0) {
            return null;
        }
        
        StoredTree tree = storage.prepare();
        tree.setID(++cNextID);
        if (currentDepth == 0) {
            tree.setName("root");
        } else {
            tree.setName("node at depth " + currentDepth);
        }
        tree.setDepth(currentDepth);

        StoredTree preceding = createTree(storage, maxDepth - 1, currentDepth + 1);
        if (preceding != null) {
            tree.setPreceding(preceding);
        }
        StoredTree subsequent = createTree(storage, maxDepth - 1, currentDepth + 1);
        if (subsequent != null) {
            tree.setSubsequent(subsequent);
        }

        tree.insert();

        return tree;
    }
    
    public abstract long getID();
    public abstract void setID(long id);

    @Nullable
    public abstract Long getPrecedingID();
    public abstract void setPrecedingID(Long id);

    @Join(internal="precedingID", external="ID")
    @Nullable
    public abstract StoredTree getPreceding() throws FetchException;
    public abstract void setPreceding(StoredTree preceding);

    @Nullable
    public abstract Long getSubsequentID();
    public abstract void setSubsequentID(Long id);

    @Join(internal="subsequentID", external="ID")
    @Nullable
    public abstract StoredTree getSubsequent() throws FetchException;
    public abstract void setSubsequent(StoredTree subsequent);
    
    public abstract String getName();
    public abstract void setName(String name);
    
    public abstract int getDepth();
    public abstract void setDepth(int depth);

    public void traverse(StoredTreeVisitor visitor) throws FetchException {
        StoredTree preceding = getPreceding();
        if (preceding != null) {
            preceding.traverse(visitor);
        }
        StoredTree subsequent = getSubsequent();
        if (subsequent != null) {
            subsequent.traverse(visitor);
        }
        visitor.visit(this);
    }
}
