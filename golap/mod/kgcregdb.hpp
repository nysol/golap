/* ////////// LICENSE INFO ////////////////////
 
 * Copyright (C) 2013 by NYSOL CORPORATION
 *
 * Unless you have received this program directly from NYSOL pursuant
 * to the terms of a commercial license agreement with NYSOL, then
 * this program is licensed to you under the terms of the GNU Affero General
 * Public License (AGPL) as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY EXPRESS OR IMPLIED WARRANTY, INCLUDING THOSE OF
 * NON-INFRINGEMENT, MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
 *
 * Please refer to the AGPL (http://www.gnu.org/licenses/agpl-3.0.txt)
 * for more details.
 
 ////////// LICENSE INFO ////////////////////*/

#ifndef kgcregdb_h
#define kgcregdb_h

#include <map>
#include <kgError.h>
#include <kgMethod.h>
#include <kgConfig.h>
#include <kgmod.h>
#include "bidx-ewah.hpp"
#include "occ.hpp"
#include "cmn.hpp"
#include "config.hpp"
#include "btree.hpp"
#include "facttable.hpp"

using namespace std;
using namespace kglib;

namespace kgmod {
    class kgCreGdb : public kgMod {
    private:
        string opt_inf;
        size_t opt_mp = 0;
        bool opt_debug = false;
        
        Config* config = NULL;
        Occ* occ = NULL;
				FactTable* factTable = NULL; 
       
    private:
        void setArgs(void);
        
    public:
        kgCreGdb(void);
        ~kgCreGdb(void);
        
        int run(void);
    };
}

#endif /* kgcregdb_h */
