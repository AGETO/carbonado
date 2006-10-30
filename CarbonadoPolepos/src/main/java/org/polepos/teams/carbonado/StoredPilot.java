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
import com.amazon.carbonado.Nullable;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Storable;

/**
 * 
 *
 * @author Brian S O'Neill
 */
@PrimaryKey("ID")
@Alias("CARBONADO_PILOT")
public interface StoredPilot extends Storable {
    int getID();
    void setID(int id);

    @Nullable
    String getName();
    void setName(String name);

    @Nullable
    String getFirstName();
    void setFirstName(String name);

    int getPoints();
    void setPoints(int points);

    int getLicenseID();
    void setLicenseID(int id);
}
