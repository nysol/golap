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
#include <boost/thread/mutex.hpp>
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
        string _liveTraFile;
        
    public:
        TraAtt* traAtt;
        ItemAtt* itemAtt;
        string occKey;              // OCCで使用するBmpListのKey名
        BTree bmpList;              //
        BTree exBmpList;            //
        Ewah liveTra;
        
        typedef vector<Ewah> occ_t; // traNo -> item bitmap
        occ_t occ;
        BTree ex_occ;
        
    private:
        boost::mutex ex_occ_mtx;
        
    public:
        Occ(Config* config, kgEnv* env);
        ~Occ(void);
        
        void buildExBmpList(void);
        void build(void);
        void saveLiveTra(void);
        void saveCooccur(const bool clean);
        void save(const bool clean);
        void loadActTra(void);
        void loadCooccur(void);
        void load(void);
        size_t LiveTraCnt(void) {return liveTra.numberOfOnes();}
        
        void item2traBmp(string& itemKey, string& itemVal, Ewah& traBmp);
        void expandItemByGranu(const size_t traNo, const vector<string>& key, const Ewah& traFilter,
                               const Ewah& itemFilter, Ewah& itemBmp,
                               unordered_map<string, Ewah>& ex_occ_CacheOnceQeuery);
        size_t itemFreq(const size_t itemNo, const Ewah& traFilter,
                        const vector<string>* tra2key = NULL);
        size_t itemFreq(size_t itemNo, vector<string>* tra2key = NULL);
        size_t attFreq(const vector<string>& attKeys, const vector<string> attVal, const Ewah& traFilter,
                       const Ewah& itemFilter, const vector<string>* tra2key = NULL);
        size_t attFreq(string& attKey, string& attVal, const Ewah& traFilter,
                       const Ewah& itemFilter, const vector<string>* tra2key = NULL);
        size_t attFreq(string attKey, string attVal, const Ewah& itemFilter,
                       const vector<string>* tra2key = NULL);

        void occ_dump(const bool debug);
        void dump(const bool debug);
        vector<string> evalKeyValue(string& key, Ewah* TraFilter = NULL) {
            return bmpList.EvalKeyValue(key, TraFilter);
        }
        size_t countKeyValue(string& key, Ewah* TraFilter = NULL) {
            return bmpList.CountKeyValue(key, TraFilter);
        }
        size_t countKeyValue(vector<string>& keys, Ewah* TraFilter = NULL) {
            return bmpList.CountKeyValue(keys, TraFilter);
        }
        size_t countKeyValue(const vector<string>& keys, Ewah& traFilter);
        void combiValues(const vector<string> flds, vector<string>& csvVals, vector<Ewah>& bmps,
                         const Ewah* traFilter = NULL) {
            return bmpList.combiValues(flds, csvVals, bmps, traFilter);
        }
        void getTra2KeyValue(string& key, vector<string>& tra2key);
        void getTra2KeyValue(const vector<string>& key, vector<string>& tra2key);
        void filterItemBmpByTraBmp(const Ewah& itemBmp, const Ewah& traBmp, Ewah& filteredItemBmp);


        vector<string> evalKeyValueItem(string& key,Ewah* ItmFilter = NULL) {
            return itemAtt->evalKeyValue(key, ItmFilter);
        }
				// Itmlistを持つtra
				/*
        Ewah* makexxx(Ewah* Itmlist,Ewah& Tra) {
        	bool first=true;
        	for (auto at2 = Itmlist->begin(), eat2 = Itmlist->end() ; at2 != eat2 ; at2++) {
	        	Ewah* tra_i2_tmp;
            bmpList.GetVal(occKey, itemAtt->item[*at2], tra_i2_tmp);
            if(first){
	            Tra = tra_i2_tmp;
	          }
	          else{
	          }
          }

        }
        */

        
        size_t sendMax(void) {return _config->sendMax;}
    };
}

#endif /* occ_hpp */
