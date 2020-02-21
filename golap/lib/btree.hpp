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

#ifndef btree_hpp
#define btree_hpp

#include <sstream>
#include <map>
#include <unordered_map>
#include <kgConfig.h>
#include <kgmod.h>
#include "bidx-ewah.hpp"
#include "btree_map.h"
#include "cmn.hpp"
#include "config.hpp"

using namespace std;

namespace kgmod {
    class BTree {

        string dbName;
        string dataTypeMapDb;
        map<string, DataType> DataTypeMap;
        map<string, size_t> card;
        
        // xxx_btree[{変数, 値}] = ビットマップ; xxx: str/num
        typedef btree::btree_map<pair<string, string>, Ewah> str_btree_t;
        typedef btree::btree_map<pair<string, double>, Ewah> num_btree_t;
        str_btree_t str_btree;
        num_btree_t num_btree;
        
        // xxx_btree_2[{変数, 値}] = レコードNo; xxx: str/num
        // High Cardinality用のbitmapを使わないインデックス
        typedef btree::btree_multimap<pair<string, string>, size_t> str_hc_btree_t;
        typedef btree::btree_multimap<pair<string, double>, size_t> num_hc_btree_t;
        str_hc_btree_t str_hc_btree;
        num_hc_btree_t num_hc_btree;
        
        unordered_map<double, string> num_str;
        
        Ewah resBmp;
        
    public:
        void PutDbName(const string dataTypeMapDb, const string dbName);
        void InitKey(const string& Key, const DataType& DataType);
        void SetBit(const string& Key, const string& KeyValue, size_t bit);
        void SetVal(const string& Key, const string& KeyValue, Ewah& Bitmap);
        bool GetVal(const string& Key, const string& KeyValue, Ewah*& Bitmap);
        bool GetVal(const string& Key, const string& KeyValue, Ewah& Bitmap);
        Ewah& GetVal(const string& Key, const string& KeyValue);
        Ewah GetVal(const vector<string> Keys, const vector<string> KeyValues);
        void GetVal(const vector<string> Keys, const vector<string> KeyValues, Ewah& Bitmap);
        Ewah& operator[](const pair<string, string>& Key) {return GetVal(Key.first, Key.second);}
        bool GetValMulti(const string& Key, const string& LikeKey, Ewah& Bitmap);
        bool GetValMulti(const string& Key, const string& Kakko, const string& FromKey,
                         const string& Kokka, const string& ToKey, Ewah& Bitmap);
        bool GetValMulti(const string& Key, const string& Operator, const string& KeyValue, Ewah& Bitmap);
        struct kvHandle {
            str_btree_t::iterator str_iter;
            num_btree_t::iterator num_iter;
            str_hc_btree_t::iterator str_hc_iter;
            num_hc_btree_t::iterator num_hc_iter;
        };
        void GetAllKeyValue(const string& Key, pair<string, Ewah>& out, kvHandle*& kvh);
        void save(bool clean);
        void load(void);
        void dump(bool debug);
        vector<string> EvalKeyValue(const string& Key, const Ewah* traFilter = NULL);
        void combiValues(const vector<string> flds, vector<string>& csvVals, vector<Ewah>& bmps,
                         const Ewah* traFilter, size_t pos = 0);
        size_t CountKeyValue(const vector<string>& Keys, const Ewah* traFilter = NULL);
        size_t CountKeyValue(const string& Key, const Ewah* traFilter = NULL);
        DataType getDataType(const string& Key);
        size_t cardinality(const string& key);
    };
}

#endif /* btree_hpp */
