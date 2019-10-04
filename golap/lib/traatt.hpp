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

#ifndef traatt_hpp
#define traatt_hpp

#include <kgConfig.h>
#include <kgmod.h>
#include "btree.hpp"
#include "cmn.hpp"
#include "config.hpp"

using namespace std;
using namespace kgmod;

namespace kgmod {
    class TraAtt {
        Config* _config;
        kgEnv* _env;
        string _dbName;
        
    public:
        btree::btree_map<string, size_t> traNo;
        btree::btree_map<size_t, string> tra;
        unordered_map<string, size_t> FldPos;
        vector<vector<char*>> traAttMap;
        size_t traMax;
        
    private:
        void loadCsv(void);
        
    public:
        TraAtt(Config* config, kgEnv* env);
        
        void build(BTree& BmpList);
        void save(bool clean = true);
        void load(void);
        void dump(bool debug);
        bool isTraAtt(const string& fldName) {return (FldPos.find(fldName) != FldPos.end());}
        vector<string> listAtt(void);
        void traNo2traAtt(const size_t traNo_, const string& traAttKey, string& traAttVal);
        void traNo2traAtt(const size_t traNo_, const vector<string>& traAttKey, vector<string>& traAttVal);
        // traattのevalKeyValueはoccのものを用いる
    };
}

#endif /* traatt_hpp */
