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

#ifndef occ_hpp
#define occ_hpp

#include <boost/functional/hash.hpp>
#include <unordered_map>
#include <map>
#include <kgConfig.h>
#include <kgmod.h>
//#include <kgArgFld.h>
#include "config.hpp"
#include "bidx-ewah.hpp"
#include "btree.hpp"
#include "traatt.hpp"
#include "itematt.hpp"

using namespace std;
using namespace kgmod;

namespace kgmod {
    class Occ {
        Config* _config;
        kgEnv* _env;
        string _dbName;
        
    public:
        traAtt* traAtt;
        itemAtt* itemAtt;
        string occKey;              // OCCで使用するBmpListのKey名
        BTree bmpList;
        
        typedef vector<Ewah> occ_t;
        occ_t occ;
        
    public:
        Occ(Config* config, kgEnv* env);
        ~Occ(void);
        
        void build(void);
        void saveCooccur(const bool clean);
        void save(const bool clean);
        void loadCooccur(void);
        void load(void);
        
        size_t itemFreq(const size_t itemNo, const Ewah& traFilter, const vector<string>* tra2key = NULL) {
            size_t cnt = 0;
            unordered_map<string, int> checkedAttVal;
            Ewah tmp;
            tmp = bmpList[{_config->traFile.itemFld, itemAtt->item[itemNo]}];
            tmp = tmp & traFilter;
            if (tra2key == NULL) {
                cnt = tmp.numberOfOnes();
            } else {
                for (auto t = tmp.begin(), et = tmp.end(); t != et; t++) {
                    string val = (*tra2key)[*t];
                    if (checkedAttVal.find(val) == checkedAttVal.end()) {
                        cnt++;
                        checkedAttVal[val] = 1;
                    }
                }
            }
            return cnt;
        }
        size_t itemFreq(size_t itemNo, vector<string>* tra2key = NULL) {
            return bmpList[{_config->traFile.itemFld, itemAtt->item[itemNo]}].numberOfOnes();
        }
        
        void occ_dump(const bool debug);
        void dump(const bool debug);
        vector<string> evalKeyValue(string& key, Ewah* TraFilter = NULL) {
            return bmpList.EvalKeyValue(key, TraFilter);
        }
        size_t countKeyValue(string& key, Ewah* TraFilter = NULL) {
            return bmpList.CountKeyValue(key, TraFilter);
        }
        void getTra2KeyValue(string& key, vector<string>* tra2key);
    };
}

#endif /* occ_hpp */
