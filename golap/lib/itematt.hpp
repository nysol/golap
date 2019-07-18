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

#ifndef itematt_hpp
#define itematt_hpp

#include <boost/algorithm/string.hpp>
#include <boost/functional/hash.hpp>
#include <kgConfig.h>
#include <kgmod.h>
#include "btree.hpp"
#include "cmn.hpp"

using namespace std;
using namespace kgmod;

namespace kgmod {
    class itemAtt {
        Config* _config;
        kgEnv* _env;
        string _dbName;
        unordered_map<pair<size_t, string>, string, boost::hash<pair<size_t, string>>> key2att_map;
        
    public:
        btree::btree_map<string, size_t> itemNo;    // マスターに示されたアイテムコード -> bitmapにおけるbit no.
        btree::btree_map<size_t, string> item;      // bitmapにおけるbit no. -> マスターに示されたアイテムコード
        btree::btree_map<size_t, string> itemName;  // bitmapにおけるbit no. -> マスターに示されたアイテム名
        size_t itemMax;
        BTree bmpList;
        
    public:
        itemAtt(Config* config, kgEnv* env);
        
        void build(void);
        void save(bool clean = true);
        void load(void);
        void loadItemTra(void);
        void dump(bool debug);
        void buildKey2attMap(void);
        void dumpKey2attMap(bool debug);
        vector<string> listAtt(void);
        vector<string> evalKeyValue(const string& key) {return bmpList.EvalKeyValue(key);}
        string key2att(const size_t _itemNo, const string& attKey) {
            if (attKey == _config->traFile.itemFld) {
                string out = item[_itemNo];
                return out;
            }
            return key2att_map[{_itemNo, attKey}];
        }
    };
}

#endif /* itematt_hpp */
