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
#include "config.hpp"
#include "bidx-ewah.hpp"
#include "btree.hpp"
#include "traatt.hpp"
#include "itematt.hpp"

using namespace std;
using namespace kgmod;

namespace kgmod {

	class Occ {
		//BTree exBmpList;

		Config* _config;
		kgEnv* _env;
		string _dbName;
		string _liveTraFile;
		// public:
		TraAtt* traAtt;
		ItemAtt* itemAtt;
		string occKey;
		BTree bmpList;
		Ewah liveTra;
        
    typedef vector<Ewah> occ_t; // traNo -> item bitmap
    occ_t occ;

    public:

		size_t traMax(void){ return traAtt->traMax ;}
		size_t itemMax(void){ return itemAtt->itemMax;}
		bool isItemAtt(string f) { return itemAtt->isItemAtt(f);}

		void getImageList(const Ewah& itemBmp, CsvFormat& imageList){ 
			itemAtt->getImageList(itemBmp, imageList);
		}
		Ewah getliveTra(void){ return liveTra; }
		
		void liveTraSet(void){
    	liveTra.padWithZeroes(traAtt->traMax + 1); 
			liveTra.inplace_logicalnot();
		}
		void liveTraSet(string v){
      Ewah tmp; 
      tmp.set(traAtt->traNo[v]);
      liveTra = liveTra - tmp;
    }



		size_t getTraID(string v){ return traAtt->traNo[v]; }
		size_t getItemID(string v){ return itemAtt->itemNo[v]; }
		
		string getTraCD(size_t i){ return traAtt->tra[i]; }
		string getItemCD(size_t i){ return itemAtt->item[i]; }
		string getItemCD(size_t i,string& f){ return itemAtt->key2att(i, f); }

		vector<string> getItemCD(size_t i,const vector<string>& f){ return itemAtt->key2att(i, f); }
		string getItemName(size_t i){ return itemAtt->itemName[i]; }
		string getItemName(size_t i,string& f){ 
			string tmp;
			itemAtt->code2name(f,itemAtt->key2att(i, f),tmp);
			return tmp;
		}

		string itemAtt_item(size_t i){ return itemAtt->item[i]; }

		// occKey == _config->traFile.itemFld みたい
		bool getTraBmpFromItem(size_t i , Ewah& tBmp){
			return bmpList.GetVal( _config->traFile.itemFld, itemAtt->item[i] ,tBmp);
		}
		Ewah& getTraBmpFromItem(size_t i ){
			return bmpList.GetVal( _config->traFile.itemFld, itemAtt->item[i]);
		}

		bool getTraBmpM(const string &key,const string kv,Ewah& tBmp){
			return bmpList.GetValMulti(key,kv,tBmp);
		}
		
		void GetAllKeyValue(const string& key, pair<string, Ewah> ret,BTree::kvHandle* kvh){
			return bmpList.GetAllKeyValue(key, ret, kvh);
		}

		void setTraBmpFromItem( string i , string t ){
			bmpList.SetBit( _config->traFile.itemFld, i , traAtt->traNo[t]);
		}

		BTree* getTraBtree(void){
			return &bmpList;
		}
		BTree* getItemBtree(void){
			return &(itemAtt->bmpList);
		}

		void resizeFromTraID(string t){

			if( traAtt->traNo[t] < occ.size() ) { return ;}
			// これOK? occ.size()の方がいい
			occ.resize(traAtt->traNo[t] + 1); 
    }

		void setItemTraBMP( string i , string t ){
			Ewah tmp;
			tmp.set(itemAtt->itemNo[i]);
			occ[traAtt->traNo[t]] = occ[traAtt->traNo[t]] | tmp;
    }

		Ewah& getTraBmp(const string& Key, const string& KeyValue){
			return bmpList.GetVal( Key, KeyValue);
		}
		Ewah getItmBmpFromGranu(const vector<string> Keys, const vector<string> KeyValues){
			return itemAtt->bmpList.GetVal( Keys, KeyValues );
		}
		Ewah getItmBmpFromTra(size_t i ){ return occ[i]; }

		void traNo2traAtt(const size_t traNo_, const string& traAttKey, string& traAttVal){
			traAtt->traNo2traAtt(traNo_, traAttKey, traAttVal);
		}
		void traNo2traAtt(const size_t traNo_, const vector<string>& traAttKey, vector<string>& traAttVal){
			traAtt->traNo2traAtt(traNo_, traAttKey, traAttVal);
		}

		btree::btree_map<string, size_t>::iterator traNoBegin(){ return traAtt->traNo.begin(); }
		btree::btree_map<string, size_t>::iterator traNoEnd(){ return traAtt->traNo.end(); }
		btree::btree_map<string, size_t>::iterator traNoFind(string v){ return traAtt->traNo.find(v); }

		btree::btree_map<string, size_t>::iterator itemNoEnd(){ return itemAtt->itemNo.end(); }
		btree::btree_map<string, size_t>::iterator itemNoFind(string v){ return itemAtt->itemNo.find(v); }

		size_t LiveTraCnt(void) {return liveTra.numberOfOnes();}



        Occ(Config* config, kgEnv* env);
        ~Occ(void);
        void build(void);
        void saveLiveTra(void);
        void saveCooccur(const bool clean);
        void save(const bool clean);
        void loadActTra(void);
        void loadCooccur(void);
        void load(void);

        
        //void item2traBmp(string& itemKey, string& itemVal, Ewah& traBmp);

        void expandItemByGranu(const size_t traNo, const vector<string>& traAttKey, const Ewah& traFilter,
                               const Ewah& itemFilter, map<size_t, Ewah>& ex_occ,
                               map<string, map<size_t, Ewah>>& ex_occ_cacheOnceQuery);



        size_t attFreq(const vector<string>& attKeys, const vector<string> attVal, const Ewah& traFilter,
                       const Ewah& itemFilter, const vector<string>* tra2key = NULL);

        size_t attFreq(string& attKey, string& attVal, const Ewah& traFilter,
                       const Ewah& itemFilter, const vector<string>* tra2key = NULL);


        void occ_dump(const bool debug);
        void dump(const bool debug);
        vector<string> evalKeyValue(string& key, Ewah* TraFilter = NULL) {
            return bmpList.EvalKeyValue(key, TraFilter);
        }
        size_t countKeyValue(string& key, Ewah* TraFilter = NULL) {
            size_t cnt;
            if (TraFilter->numberOfOnes() == traAtt->traMax + 1) {
                cnt = bmpList.CountKeyValue(key, NULL);
            } else {
                cnt = bmpList.CountKeyValue(key, TraFilter);
            }
            return cnt;
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

        size_t ibmpList_GetVal(const vector<string>& attKey,const vector<string> attVal,Ewah *itemBmp){
					return itemAtt->bmpList.GetVal(attKey[0], attVal[0], itemBmp);
				}	
        size_t sendMax(void) {return _config->sendMax;}
    };
}

#endif /* occ_hpp */
