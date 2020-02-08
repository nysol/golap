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

#include <stdio.h>
#include <cstdlib>
#include <fstream>
#include <kgError.h>
#include <kgMethod.h>
#include <kgConfig.h>
#include <kgMessage.h>
#include <chrono>
#include <cfloat>
#include <set>
#include <stack>
#include <boost/algorithm/string.hpp>
#include <boost/functional/hash.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include "kgCSV.h"
#include "bidx-ewah.hpp"
#include "cmn.hpp"
#include "config.hpp"
#include "param.hpp"
#include "cmdcache.hpp"
#include "filter.hpp"
//#include "http.hpp"
#include "kggolap.hpp"

using namespace std;
using namespace kgmod;

// -----------------------------------------------------------------------------
// コンストラクタ(モジュール名，バージョン登録,パラメータ)
// -----------------------------------------------------------------------------
kgmod::kgGolap::kgGolap(void)
{
/*
    _name    = "golap";
    _version = "###VERSION###";
    
#include <help/en/kggolapHelp.h>
    _titleL = _title;
    _docL   = _doc;
#ifdef JPN_FORMAT
#include <help/jp/kggolapHelp.h>
#endif
*/
}

kgmod::kgGolap::~kgGolap(void) {
    if (occ != NULL) delete occ;
    if (config != NULL) delete config;
};

// -----------------------------------------------------------------------------
// 引数の設定
// -----------------------------------------------------------------------------
Result kgmod::Enum(QueryParams& query, Ewah& dimBmp ,size_t tlimit=45) {

	// Timer 設置
	int isTimeOut=0;
	pthread_t pt;
	timChkT timerST;
	timerST.isTimeOut = &isTimeOut;
	timerST.timerInSec = tlimit;

	if (tlimit){ // 要エラーチェック
		cerr << "setTimer: " << tlimit << " sec" << endl;
		pthread_create(&pt, NULL, timerLHandle, &timerST);			
	}

	string headstr = "node1,node2,frequency,frequency1,frequency2,total,support,confidence,lift,jaccard,PMI,node1n,node2n";
	vector<string> csvHeader = splitToken(headstr,',');
	Result res(csvHeader.size());
	res.setHeader(csvHeader);

    cerr << "enumerating" << endl;
    size_t hit = 0;
    bool notSort = false;
    signed int stat = 0;
    unordered_map<size_t, size_t> itemFreq;
    
    Ewah tarTraBmp = query.traFilter & dimBmp & mt_occ->liveTra;
    size_t traNum = tarTraBmp.numberOfOnes();
    if (traNum == 0) {
        cerr << "trabitmap is empty" << endl;
        res.setSTS(0,0,0);
				if (tlimit){ 
    		  if (!isTimeOut) {
						pthread_cancel(pt);
						cerr << "timer canceled" << endl;
					}
				}

        return res;
    }
    //++++++++++
    Ewah traBmpInFact;
    Ewah itemBmpInFact;

    mt_factTable->toTraItemBmp(query.factFilter, query.itemFilter, traBmpInFact, itemBmpInFact);
    cerr << "tra in fact "; Cmn::CheckEwah(traBmpInFact);

    tarTraBmp = tarTraBmp & traBmpInFact;
    cerr << "tarTraBmp "; Cmn::CheckEwah(tarTraBmp);

    Ewah tarItemBmp = query.itemFilter & itemBmpInFact;
    cerr << "tarItemBmp "; Cmn::CheckEwah(tarItemBmp);

    //----------

    // granularityのsizeが1で、かつ、[0]がキーであれば、granularityを指定していない(=false)
    bool isTraGranu  = (query.granularity.first.size() != 1) ||
                        (query.granularity.first[0] != mt_config->traFile.traFld);
    bool isNodeGranu = (query.granularity.second.size() != 1) ||
                        (query.granularity.second[0] != mt_config->traFile.itemFld);

    //++++++++++
    map<vector<string>, size_t> itemNo4node_map;    // [node] -> coitemsをカウントする代表itemNo
    //----------

    vector<string> tra2key;     // [traNo] -> 当該トランザクションが有するTraAttの値リスト(csv)
    // transaction granularityを指定していた場合は、traNumを指定したtraAttの集計値で上書きする

    if (isTraGranu) {
        // vector版countKeyValueは処理が比較的重いので、マルチバリューかどうかで処理を分ける
        if (query.granularity.first.size() == 1) {
            //++++++++++
            traNum = mt_occ->countKeyValue(query.granularity.first[0], &query.traFilter);
            //traNum = mt_occ->countKeyValue(query.granularity.first[0], &tarTraBmp);
            //----------
            mt_occ->getTra2KeyValue(query.granularity.first[0], tra2key);
        } else {
            //++++++++++
            traNum = mt_occ->countKeyValue(query.granularity.first, query.traFilter);
            //traNum = mt_occ->countKeyValue(query.granularity.first, tarTraBmp);
            //----------
            mt_occ->getTra2KeyValue(query.granularity.first, tra2key);
        }
    }


    //++++++++++
    map<string, map<size_t, Ewah>> ex_occ_cacheOnceQuery;      // ["field name"] -> {traNo, occ(itemBmp)}

		map<string, Ewah> ex_occ_cacheOnceQuery0;
    set<vector<string>> checked_node2;  // [vnodes] -> exists



		// DEBUG		
		size_t icnt =query.itemFilter.numberOfOnes();
		int cnt=1;

   // unordered_map<string, Ewah> ex_occ_cacheOnceQuery;      // ["field name"] -> item bitmap
    //unordered_map<vector<string>, bool, boost::hash<vector<string>>> checked_node2;  // [vnodes] -> exists
    //for (auto i2 = query.itemFilter.begin(), ei2 = query.itemFilter.end(); i2 != ei2; i2++) {
		cerr << "icnt "<< tarItemBmp.numberOfOnes() << endl; 
    for (auto i2 = tarItemBmp.begin(), ei2 = tarItemBmp.end(); i2 != ei2; i2++) {
				// CHECK 用
		    if ( cnt%1000==0 ){ cerr << cnt << "/" << icnt << endl;}
		    cnt++;

        if (isTimeOut) {stat = 2; break;}
        vector<string> vnode2;
        string node2;
        if (isNodeGranu) {
            // itemNo(2)から指定した粒度のキー(node2)を作成する。
            vnode2 = mt_occ->itemAtt->key2att(*i2, query.granularity.second);
            node2 = Cmn::CsvStr::Join(vnode2, ":");
        } else {
            node2 = mt_occ->itemAtt->item[(*i2)];
            vnode2.resize(1);
            vnode2[0] = node2;
        }

        // すでに処理済みのキーの場合はcontinueする。
        if (checked_node2.find(vnode2) == checked_node2.end()) {
            //++++++++++
            checked_node2.insert(vnode2);
            //checked_node2[vnode2] = true;
            //----------
        } else {
            continue;
        }

        size_t itemNo4node2;
        if (isNodeGranu) {
            // node2における代表itemNoとして拡張前のitemNo(2)を設定する
            if (itemNo4node_map.find(vnode2) == itemNo4node_map.end()) {
                itemNo4node_map[vnode2] = *i2;
                itemNo4node2 = *i2;
            } else {
                itemNo4node2 = itemNo4node_map[vnode2];
            }
        } else {
            itemNo4node2 = *i2;
        }

        if (itemFreq.find(itemNo4node2) == itemFreq.end()) {
        //----------
            if (isTraGranu) {
                //++++++++++
                itemFreq[itemNo4node2] = mt_occ->attFreq(query.granularity.second, vnode2,
                                                tarTraBmp, tarItemBmp, &tra2key);
                //itemFreq[*i2] = mt_occ->attFreq(query.granularity.second, vnode2,
                //                                tarTraBmp, query.itemFilter, &tra2key);
                //----------
            } else {
                //++++++++++
                itemFreq[itemNo4node2] = mt_occ->attFreq(query.granularity.second, vnode2,
                                                tarTraBmp, tarItemBmp);
                //itemFreq[*i2] = mt_occ->attFreq(query.granularity.second, vnode2,
                //                                tarTraBmp, query.itemFilter);
                //----------
            }
        }

        //++++++++++
        if (  ((float)itemFreq[itemNo4node2]/traNum) < query.selCond.minSup ){
            continue;
        }

        if (itemFreq[itemNo4node2] == 0) continue;
        

        // すでに処理済みのキーの場合はcontinueする。
        //if (checked_node2.find(vnode2) == checked_node2.end()) {
        //    checked_node2[vnode2] = true;
        //} else {
        //    continue;
        //}
        

        map<size_t, size_t> coitems;
        set<pair<string, string>> checked_node1;    // [vnodes, traAtt(:区切り)] -> exists
        //unordered_map<pair<string, string>, bool, boost::hash<pair<string, string>>> checked_node1;
        //                                                    // [vnodes, traAtt(:区切り)] -> exists
        //unordered_map<pair<size_t, string>, bool, boost::hash<pair<size_t, string>>> checked_item1;
         //                                                   // [itemNo, traAtt(:区切り)] -> exists
        //unordered_map<vector<string>, size_t, boost::hash<vector<string>>> itemNo4node_map;
         //                                                   // [node] -> coitemsをカウントする代表itemNo
        // itemNo(2)の属性(vnode2)から、requestで指定された粒度で対象itemNoを拡張する(itemInTheAtt2)


        Ewah itemInTheAtt2 = mt_occ->itemAtt->bmpList.GetVal(query.granularity.second, vnode2);
        //++++++++++
        itemInTheAtt2 = itemInTheAtt2 & tarItemBmp;
        //itemInTheAtt2 = itemInTheAtt2 & query.itemFilter;
        //----------
				//--- sp2 sp2
	/*
				cerr << "spx1" <<  endl;
				Ewah tra_i2_tmp1;
				for (auto at2 = itemInTheAtt2.begin(), eat2 = itemInTheAtt2.end(); at2 != eat2; at2++) {
					if (isTimeOut) {stat = 2; break;}
					Ewah* tra_i2_tmp2; 
					mt_occ->bmpList.GetVal(mt_occ->occKey, mt_occ->itemAtt->item[*at2], tra_i2_tmp2);
					tra_i2_tmp1 = tra_i2_tmp1 | *tra_i2_tmp2;
				}
				// item2 の持つtra LIST
				Ewah tra_i2 = tra_i2_tmp1 & tarTraBmp;

				cerr << "spx2" <<  endl;

				// ここおそくなりそうbmpで一気する方法はある？ 
				unordered_map<vector<string>, bool , boost::hash<vector<string>>> checked_tra1;

        for (auto t2 = tra_i2.begin(), et2 = tra_i2.end(); t2 != et2; t2++) {
          if (isTimeOut) {stat = 2; break;}

					vector<string> vTraAtt;
					mt_occ->traAtt->traNo2traAtt(*t2, query.granularity.first, vTraAtt);
					string traAtt = Cmn::CsvStr::Join(vTraAtt, ":");

					if (checked_tra1.find(vTraAtt) != checked_tra1.end()) { continue; }
					checked_tra1[vTraAtt] = true;

          //+++++++++
          // どっかでする方法考える
          // queryのfactFilterに，traNo(*t2)と拡張済itemNo(*at2)の組み合わせがなければcontinue
          //if (!mt_factTable->existInFact(*t2, *at2, query.factFilter)) continue;
          //----------
          
          //++++++++++
          //map<size_t, Ewah> item_i1;      // [traNo] -> occ(itemBmp)
          map<size_t, pair<size_t, Ewah>> itemNo4node1_itemNo;      // [代表itemNo] -> {itemNo, traBmp}
          Ewah item_i1;
          //----------

          //----------
          if (isTraGranu) {
          	// t2の属性から、requestで指定された粒度で対象トランザクションを拡張した上で、対象itemNo(1)を拡張する
						//++++++++++
						//テスト的に除去 
						//	mt_occ->expandItemByGranu(*t2, query.granularity.first, tarTraBmp,
            //                        tarItemBmp, item_i1, ex_occ_cacheOnceQuery0);
						//テスト的に除去 
            
                    //mt_occ->expandItemByGranu(*t2, query.granularity.first, tarTraBmp,
                    //                          query.itemFilter, item_i1, ex_occ_cacheOnceQuery);
                    //----------
          } else {
             //++++++++++
              //item_i1.insert({*t2, mt_occ->occ[*t2] & tarItemBmp});
              item_i1 = mt_occ->occ[*t2] & query.itemFilter;
                    //----------
  	      }

					for (auto i1 = item_i1.begin(), ei1 = item_i1.end(); i1 != ei1; i1++) {
		      	if (isTimeOut) {stat = 2; break;}
	  	      vector<string> vnode1 = mt_occ->itemAtt->key2att(*i1, query.granularity.second);
	  	      //mt_occ->itemAtt->key2att(*ii1_item, query.granularity.second);
	  	      
  	  	    string node1 = Cmn::CsvStr::Join(vnode1, ":");

       		 if (checked_node1.find({node1, traAtt}) == checked_node1.end()) {
	       		 checked_node1.insert({node1, traAtt});
 						} else {
						continue;
						}
		 				// itemNo(1)が持つ属性(vnode1)から、requestで指定された粒度で対象itemNoを拡張する(itemInTheAtt1)
						Ewah itemInTheAtt1 = mt_occ->itemAtt->bmpList.GetVal(query.granularity.second, vnode1);
						itemInTheAtt1 = itemInTheAtt1 & tarItemBmp;
	
		        if (*(itemInTheAtt1.begin()) >= *(itemInTheAtt2.begin())) continue;

						size_t itemNo4node;
						if (itemNo4node_map.find(vnode1) == itemNo4node_map.end()) {
        			itemNo4node_map[vnode1] = *itemInTheAtt1.begin();
         			 itemNo4node = *itemInTheAtt1.begin();
	       		 } else {
  	        		itemNo4node = itemNo4node_map[vnode1];
				      }	
							coitems[itemNo4node]++;
          
          }

				}
				*/


				// ===== sp2 sp2 org
        for (auto at2 = itemInTheAtt2.begin(), eat2 = itemInTheAtt2.end(); at2 != eat2; at2++) {
            if (isTimeOut) {stat = 2; break;}
            Ewah* tra_i2_tmp;
            mt_occ->bmpList.GetVal(mt_occ->occKey, mt_occ->itemAtt->item[*at2], tra_i2_tmp);
            Ewah tra_i2 = *tra_i2_tmp & tarTraBmp;
            
            for (auto t2 = tra_i2.begin(), et2 = tra_i2.end(); t2 != et2; t2++) {
                if (isTimeOut) {stat = 2; break;}

                //++++++++++
                // queryのfactFilterに，traNo(*t2)と拡張済itemNo(*at2)の組み合わせがなければcontinue
                if (!mt_factTable->existInFact(*t2, *at2, query.factFilter)) continue;
                //----------


                vector<string> vTraAtt;
                mt_occ->traAtt->traNo2traAtt(*t2, query.granularity.first, vTraAtt);
                string traAtt = Cmn::CsvStr::Join(vTraAtt, ":");
                
                //++++++++++
                map<size_t, Ewah> item_i1;      // [traNo] -> occ(itemBmp)
                map<size_t, pair<size_t, Ewah>> itemNo4node1_itemNo;      // [代表itemNo] -> {itemNo, traBmp}
                //Ewah item_i1;
                //----------

                //----------
                if (isTraGranu) {
                    // t2の属性から、requestで指定された粒度で対象トランザクションを拡張した上で、対象itemNo(1)を拡張する
                    //++++++++++
                    mt_occ->expandItemByGranu(*t2, query.granularity.first, tarTraBmp,
                                              tarItemBmp, item_i1, ex_occ_cacheOnceQuery);
                    //mt_occ->expandItemByGranu(*t2, query.granularity.first, tarTraBmp,
                    //                          query.itemFilter, item_i1, ex_occ_cacheOnceQuery);
                    //----------
                } else {
                    //++++++++++
                    item_i1.insert({*t2, mt_occ->occ[*t2] & tarItemBmp});
                    //item_i1 = mt_occ->occ[*t2] & query.itemFilter;
                    //----------
                }


                //++++++++++
                for (auto ii1 = item_i1.begin(); ii1 != item_i1.end(); ii1++) {
                    // ii1: first=traNo, second=occ(itemBmp)
                    for (auto ii1_item = ii1->second.begin(), eii1_item1 = ii1->second.end();
                         ii1_item != eii1_item1; ii1_item++) {
                        vector<string> vnode1 = mt_occ->itemAtt->key2att(*ii1_item, query.granularity.second);
                        // itemNo(1)が持つ属性(vnode1)から、requestで指定された粒度で対象itemNoを拡張する(itemInTheAtt1)
                        Ewah itemInTheAtt1 = mt_occ->itemAtt->bmpList.GetVal(query.granularity.second, vnode1);
                        itemInTheAtt1 = itemInTheAtt1 & tarItemBmp;
                        auto topBit = itemInTheAtt1.begin();
                        
                        if (itemNo4node1_itemNo.find(*topBit) == itemNo4node1_itemNo.end()) {
                            Ewah tmpTraBmp;
                            tmpTraBmp.set(ii1->first);
                            itemNo4node1_itemNo[*topBit] = {*ii1_item, tmpTraBmp};
                        } else {
                            itemNo4node1_itemNo[*topBit].second.set(ii1->first);
                        }
                    }
                }
                //----------

                //++++++++++
                for (auto it = itemNo4node1_itemNo.begin(), eit = itemNo4node1_itemNo.end(); it != eit; it++) {
                    size_t itemNo4node1 = it->first;
                    size_t i1 = it->second.first;
                //for (auto i1 = item_i1.begin(), ei1 = item_i1.end(); i1 != ei1; i1++) {
                //----------
                    if (isTimeOut) {stat = 2; break;}
                    //++++++++++
                    if (itemNo4node1 >= *i2) break;
                    vector<string> vnode1 = mt_occ->itemAtt->key2att(i1, query.granularity.second);
                    //if (*i1 >= *i2) break;
                    //vector<string> vnode1 = mt_occ->itemAtt->key2att(*i1, query.granularity.second);
                    //----------
                    string node1 = Cmn::CsvStr::Join(vnode1, ":");
                    if (node1 == node2) continue;
                    
                    //++++++++++
                    if (checked_node1.find({node1, traAtt}) == checked_node1.end()) {
                        checked_node1.insert({node1, traAtt});
                    //if (checked_node1.find({node1, traAtt}) == checked_node1.end()) {
                    //    checked_node1[{node1, traAtt}] = true;
                    //----------
                    } else {
                        continue;
                    }
                    
                    //++++++++++
                    // itemNo(1)が持つ属性(vnode1)から、requestで指定された粒度で対象itemNoを拡張する(itemInTheAtt1)
                    Ewah itemInTheAtt1;
                    if (isNodeGranu) {
                        itemInTheAtt1 = mt_occ->itemAtt->bmpList.GetVal(query.granularity.second, vnode1);
                    } else {
                        // 拡張は不要
                        itemInTheAtt1.set(i1);
                    }
                    //Ewah itemInTheAtt1 = mt_occ->itemAtt->bmpList.GetVal(query.granularity.second, vnode1);
                    //----------

                    //++++++++++
                    // node1における代表itemNoとして拡張前のitemNo(1)を設定する
                    if (itemNo4node_map.find(vnode1) == itemNo4node_map.end()) {
                        itemNo4node_map[vnode1] = itemNo4node1;
                    } else {
                        itemNo4node1 = itemNo4node_map[vnode1];
                    }
                    //----------
                    
                    for (auto at1 = itemInTheAtt1.begin(), eat1 = itemInTheAtt1.end(); at1 != eat1; at1++) {
                        if (isTimeOut) {stat = 2; break;}
                        
                        //++++++++++
                        //size_t itemNo4node1;
                        //if (isNodeGranu) {
                        //    // node1における代表itemNoとして拡張前のitemNo(1)を設定する
                        //    if (itemNo4node_map.find(vnode1) == itemNo4node_map.end()) {
                        //     itemNo4node_map[vnode1] = *i1;
                        //     itemNo4node1 = *i1;
                        //    } else {
                        //        itemNo4node1 = itemNo4node_map[vnode1];
                        //    }
                        //} else {
                        //    itemNo4node1 = *i1;
                        //}
                        //----------
                        
                        //++++++++++
                        // queryのfactFilterに，traNo(*t2)と拡張済itemNo(*at2)の組み合わせがあれば共起としてカウント
                        if (mt_factTable->existInFact(it->second.second, *at1, query.factFilter)) {
                            coitems[itemNo4node1]++;
                            break;
                        }
                        // coitems[itemNo4node1]++;
                        //----------
                    }
                }
            }
        }
				// =====
        
        
        const string delim = ":";
        for (auto i1 = coitems.begin(), ei1 = coitems.end(); i1 != ei1; i1++) {
            if (isTimeOut) {stat = 2; break;}
            string item1, item2;
            string itemName1, itemName2;
            for (auto& f : query.granularity.second) {
                if (f == mt_config->traFile.itemFld) {
                    item1 += mt_occ->itemAtt->item[i1->first] + delim;
                    item2 += mt_occ->itemAtt->item[*i2] + delim;
                    itemName1 += mt_occ->itemAtt->itemName[i1->first] + delim;
                    itemName2 += mt_occ->itemAtt->itemName[*i2] + delim;
                } else {
                    item1 += mt_occ->itemAtt->key2att(i1->first, f) + delim;
                    item2 += mt_occ->itemAtt->key2att(*i2, f) + delim;
                    string tmp1, tmp2;
                    mt_occ->itemAtt->code2name(f, mt_occ->itemAtt->key2att(i1->first, f), tmp1);
                    mt_occ->itemAtt->code2name(f, mt_occ->itemAtt->key2att(*i2, f), tmp2);
                    itemName1 += tmp1 + delim;
                    itemName2 += tmp2 + delim;
                }
            }
            Cmn::EraseLastChar(item1);
            Cmn::EraseLastChar(item2);
            Cmn::EraseLastChar(itemName1);
            Cmn::EraseLastChar(itemName2);
            
            size_t freq = i1->second;
            float sup = (float)freq / traNum;
            if (sup < query.selCond.minSup) continue;
            /*
            if (query.granularity.second.size() == 1 &&
                query.granularity.second[0] == mt_config->traFile.itemFld) {
                if (query.granularity.first.size() == 1 &&
                    query.granularity.first[0] == mt_config->traFile.traFld) {
                    if (itemFreq.find(i1->first) == itemFreq.end()) {
                        itemFreq[i1->first] = mt_occ->itemFreq(i1->first, tarTraBmp);
                    }
                    if (itemFreq.find(*i2) == itemFreq.end()) {
                        itemFreq[*i2] = mt_occ->itemFreq(*i2, tarTraBmp);
                    }
                } else {
                    if (itemFreq.find(i1->first) == itemFreq.end()) {
                        itemFreq[i1->first] = mt_occ->itemFreq(i1->first, tarTraBmp, &tra2key);
                    }
                    if (itemFreq.find(*i2) == itemFreq.end()) {
                        itemFreq[*i2] = mt_occ->itemFreq(*i2, tarTraBmp, &tra2key);
                    }
                }
            } else {
                vector<string> attVal1 = mt_occ->itemAtt->key2att(i1->first, query.granularity.second);
                vector<string> attVal2 = mt_occ->itemAtt->key2att(*i2, query.granularity.second);
                if (query.granularity.first.size() == 1 &&
                    query.granularity.first[0] == mt_config->traFile.traFld) {
                    if (itemFreq.find(i1->first) == itemFreq.end()) {
                        if (isTraGranu) {
                            itemFreq[i1->first] = mt_occ->attFreq(query.granularity.second[0],
                                                                  attVal1[0], tarTraBmp, &tra2key);
                        } else {
                            itemFreq[i1->first] = mt_occ->attFreq(query.granularity.second[0],
                                                                  attVal1[0], tarTraBmp);
                        }
                    }
                    if (itemFreq.find(*i2) == itemFreq.end()) {
                        if (isTraGranu) {
                            itemFreq[*i2] = mt_occ->attFreq(query.granularity.second[0], attVal2[0],
                                                            tarTraBmp, &tra2key);
                        } else {
                            itemFreq[*i2] = mt_occ->attFreq(query.granularity.second[0], attVal2[0], tarTraBmp);
                        }
                    }
                } else {
                    if (itemFreq.find(i1->first) == itemFreq.end()) {
                        itemFreq[i1->first] = mt_occ->attFreq(query.granularity.second, attVal1,
                                                              tarTraBmp, query.itemFilter, &tra2key);
                    }
                    if (itemFreq.find(*i2) == itemFreq.end()) {
                        itemFreq[*i2] = mt_occ->attFreq(query.granularity.second, attVal2,
                                                        tarTraBmp, query.itemFilter, &tra2key);
                    }
                }
            }*/
            float conf1 = (float)freq / itemFreq[i1->first];
            if (conf1 < query.selCond.minConf) continue;
            
            float jac = (float)freq / (itemFreq[i1->first] + itemFreq[*i2] - freq);
            if (jac < query.selCond.minJac) continue;
            double lift = (double)(freq * traNum) / (itemFreq[i1->first] * itemFreq[*i2]);
            if (lift < query.selCond.minLift) continue;
            
            float pmi = Cmn::calcPmi(freq, itemFreq[i1->first], itemFreq[*i2], traNum);
            if (pmi < query.selCond.minPMI) continue;
            
            //char msg[1024];
            // node1,node2,frequency,frequency1,frequency2,total,support,confidence,lift,jaccard,PMI,node1n,node2n
            //const char* fmt = "%s,%s,%d,%d,%d,%d,%.5f,%.5f,%.5lf,%.5f,%.5f,%s,%s";
            //sprintf(msg, fmt, item1.c_str(), item2.c_str(), freq, itemFreq[i1->first], itemFreq[*i2], traNum,
            //        sup, conf1, lift, jac, pmi, itemName1.c_str(), itemName2.c_str());
						vector<string> dt(13);
						dt[0] = item1; 
						dt[1] = item2; 
						dt[2] = toString(freq); 
						dt[3] = toString(itemFreq[i1->first]); 
						dt[4] = toString(itemFreq[*i2]); 
						dt[5] = toString(traNum); 
						char conv[64];
						sprintf(conv,"%.5f",sup);   dt[6] = conv; 
						sprintf(conv,"%.5f",conf1); dt[7] = conv; 
						sprintf(conv,"%.5lf",lift); dt[8] = conv; 
						sprintf(conv,"%.5f",jac);   dt[9] = conv; 
						sprintf(conv,"%.5f",pmi);   dt[10]= conv; 
						dt[11] = itemName1; 
						dt[12] = itemName2; 

            float skey;
            if (query.sortKey == SORT_SUP) {
                skey = -sup;
            } else if (query.sortKey == SORT_CONF) {
                skey = -conf1;
            } else if (query.sortKey == SORT_LIFT) {
                skey = -lift;
            } else if (query.sortKey == SORT_JAC) {
                skey = -jac;
            } else if (query.sortKey == SORT_PMI) {
                skey = -pmi;
            } else {
                notSort = true;
                skey = -10;
            }
            res.insert(make_pair(skey,dt));
            if (res.size() > query.sendMax) {
            	res.pop();
              stat = 1;
              if (notSort) break;
            }
            hit++;
        }

    }

		if (tlimit){ 
      if (!isTimeOut) {
				pthread_cancel(pt);
				cerr << "timer canceled" << endl;
			}
		}
		res.setSTS(stat,res.size(),hit);
		res.showSTS();

		return res;
}

void kgmod::MT_Enum(mq_t* mq, QueryParams* query, map<string, Result>* res ,unsigned int *dl) {
    mq_t::th_t* T = mq->pop();
    while (T != NULL) {
        cerr << "#" << T->first << ") tarTra:"; Cmn::CheckEwah(T->second.second);
        Result rr = Enum(*query, *(T->second.second),*dl);
        (*res)[T->second.first] = rr;
        delete T->second.second;
        delete T;
        T = mq->pop();
    }
}
map<string, Result> kgmod::kgGolap::runQuery(
		string traFilter,string itemFilter,
		string factFilter,
		string gTransaction,string gNode,
		string SelMinSup,string SelMinConf,string SelMinLift,
		string SelMinJac,string SelMinPMI,
		string sortKey,string sendMax,string dimension,string deadline
){

	QueryParams qPara(
		occ->traAtt->traMax , 
		occ->itemAtt->itemMax ,
		factTable->recMax,
		config->sendMax,
		config->traFile.traFld ,
		config->traFile.itemFld
	);

	if(!traFilter.empty()){
		qPara.traFilter = fil->makeTraBitmap(traFilter);
	}
	if(!itemFilter.empty()){
		qPara.itemFilter = fil->makeItemBitmap(itemFilter);
	}
	if(!factFilter.empty()){
  	qPara.factFilter = fil->makeFactBitmap(factFilter);
  }

	if(!gTransaction.empty()){
		vector<string> buf = Cmn::CsvStr::Parse(gTransaction.c_str());
		if (! buf.empty()) {
			qPara.granularity.first.clear();
			qPara.granularity.first.reserve(buf.size());
			for (auto& f : buf) {
				if(!config->isinTraGranuFLD(f)){
					string msg = "nodeimage.granularity.transaction(";
					msg += gTransaction;
					msg += ") must be set in config file (traAttFile.granuFields)\n";
					throw kgError(msg);
				}
				qPara.granularity.first.push_back(f);
			}
		}
	}
	if(!gNode.empty()){
		vector<string> buf = Cmn::CsvStr::Parse(gNode.c_str());
		if (! buf.empty()) {
			qPara.granularity.second.clear();
			qPara.granularity.second.reserve(buf.size());
			for (auto& f : buf) {
				if (! occ->itemAtt->isItemAtt(f)) {
					string msg = f;
					msg += " is not item attribute\n";
					throw kgError(msg);
				}
				qPara.granularity.second.push_back(f);
			}
		}
	}

	if(!dimension.empty()){
    vector<string> param = Cmn::CsvStr::Parse(dimension);
    qPara.dimension.key = param[0];
    for (size_t i = 1; i < param.size(); i++) {
      qPara.dimension.DimBmpList[param[i]] = occ->bmpList.GetVal(qPara.dimension.key, param[i]);
    }
  }

	if (  boost::iequals(sortKey , "SUP")){
		qPara.sortKey = SORT_SUP;
	}else if(  boost::iequals(sortKey ,"CONF")){
		qPara.sortKey = SORT_CONF;
	}else if(  boost::iequals(sortKey,"LIFT")){
		qPara.sortKey = SORT_LIFT;
	}else if(  boost::iequals(sortKey ,"JAC")){
		qPara.sortKey = SORT_JAC;
	}else if ( boost::iequals(sortKey , "PMI")){
		qPara.sortKey = SORT_PMI;
	}else{
		qPara.sortKey = SORT_NONE;
	}
	if(!sendMax.empty()){
		qPara.sendMax = atoll(sendMax.c_str());
	}
	// selCond
	if(!SelMinSup.empty()){
		qPara.selCond.minSup = atof(SelMinSup.c_str());
	}
	if(!SelMinConf.empty()){
		qPara.selCond.minConf = atof(SelMinConf.c_str());
	}
	if(!SelMinLift.empty()){
		qPara.selCond.minLift = atof(SelMinLift.c_str());
	}
	if(!SelMinJac.empty()){
		qPara.selCond.minJac = atof(SelMinJac.c_str());
	}
	if(!SelMinPMI.empty()){
		qPara.selCond.minPMI = atof(SelMinPMI.c_str());
	}

	unsigned int dline = config->deadlineTimer;
	if(! deadline.empty()){
		dline = atol(deadline.c_str());
	}

	map<string, Result> res;

	if (qPara.dimension.DimBmpList.size() == 0) {	
		res[""] =kgmod::Enum(qPara, mt_occ->liveTra,dline); 
	}
	else{

		if (mt_config->mt_enable) {
			mq_t::th_t *th;
			mq_t mq;
			size_t threadNo = 0;
			for (auto i = qPara.dimension.DimBmpList.begin(); i != qPara.dimension.DimBmpList.end(); i++) {
				cerr << "running with multi-threading (";
				cerr << threadNo << " of " << qPara.dimension.DimBmpList.size() << ")" << endl;
				th = new mq_t::th_t;
				th->first = threadNo;
				th->second.first = i->first;
				th->second.second = new Ewah;
				th->second.second->expensive_copy(i->second);
				mq.push(th);
				threadNo++;
			}
   	 vector<boost::thread> thg;
   	 for (int i = 0; i < mt_config->mt_degree; i++) {
				thg.push_back(
					boost::thread([&mq, &qPara, &res,&dline] {MT_Enum(&mq, &qPara, &res ,&dline);})
				);
			}
			for (boost::thread& th : thg) { th.join();}
  	}
  	else{
    	for (auto i = qPara.dimension.DimBmpList.begin(); i != qPara.dimension.DimBmpList.end(); i++) {
      	res[i->first] = kgmod::Enum(qPara, i->second,dline);
	    }
		}

	}

	return res;
	
}

vector< vector<string> > kgmod::kgGolap::nodestat(
	string traFilter,string itemFilter,
	string factFilter,
	string gTransaction,string gNode,
	string itemVal,string values
) {

	NodeStatParams nSpara(
		occ->traAtt->traMax , 
		occ->itemAtt->itemMax ,
		factTable->recMax,
		config->traFile.traFld ,
		config->traFile.itemFld
	);

	if(!traFilter.empty()){
		nSpara.traFilter = fil->makeTraBitmap(traFilter);
	}
	if(!itemFilter.empty()){
		nSpara.itemFilter = fil->makeItemBitmap(itemFilter);
	}
	if(!factFilter.empty()){
  	nSpara.factFilter = fil->makeFactBitmap(factFilter);
  }

	if(!gTransaction.empty()){
		vector<string> buf = Cmn::CsvStr::Parse(gTransaction.c_str());
		if (! buf.empty()) {
			nSpara.granularity.first.clear();
			nSpara.granularity.first.reserve(buf.size());
			for (auto& f : buf) {
				if(!config->isinTraGranuFLD(f)){
					string msg = "nodeimage.granularity.transaction(";
					msg += gTransaction;
					msg += ") must be set in config file (traAttFile.granuFields)\n";
					throw kgError(msg);
				}
				nSpara.granularity.first.push_back(f);
			}
		}
	}
	if(!gNode.empty()){
		vector<string> buf = Cmn::CsvStr::Parse(gNode.c_str());
		if (! buf.empty()) {
			nSpara.granularity.second.clear();
			nSpara.granularity.second.reserve(buf.size());
			for (auto& f : buf) {
				if (! occ->itemAtt->isItemAtt(f)) {
					string msg = f;
					msg += " is not item attribute\n";
					throw kgError(msg);
				}
				nSpara.granularity.second.push_back(f);
			}
		}
	}

	if(!itemVal.empty()){
		nSpara.itemVal.reserve(1);
		nSpara.itemVal.push_back(itemVal);
	}
	else {
		throw kgError("nodestat.itemVal must be set in request\n");
	}

	if(!values.empty()){
		vector<string> vec = Cmn::CsvStr::Parse(values.c_str());
		nSpara.vals.resize(vec.size());

		for (size_t i = 0; i < vec.size(); i++) {
			nSpara.vals[i].first.setValPosMap(factTable->valPosMap());
			vector<string> v = Cmn::Split(vec[i], ':');
			nSpara.vals[i].first.parse(v[0]);
      if      (v.size() == 1) { nSpara.vals[i].second = v[0];} 
      else if (v.size() == 2) { nSpara.vals[i].second = v[1];} 
      else { throw kgError("error in nodestat.values"); }
		}
	}
	else{	
		throw kgError("nodestat.value must be set in request\n");
	}
	
	vector< vector<string> > rnb;

	//signed int stat = 0;
	vector<string> header;
	header.push_back(Cmn::CsvStr::Join(nSpara.granularity.second, ":")) ;
	for (auto& v : nSpara.vals) {
		header.push_back(v.second);
	}
	rnb.push_back(header);

	string traHeader = "";

	Ewah itemBmp;
	mt_occ->itemAtt->bmpList.GetVal(nSpara.granularity.second, nSpara.itemVal, itemBmp);
	itemBmp = itemBmp & nSpara.itemFilter;
	string itemValj = Cmn::CsvStr::Join(nSpara.itemVal, ":");

	mt_factTable->aggregate(
		{traHeader, nSpara.traFilter}, 
		{itemValj, itemBmp},
		nSpara.vals, rnb);

	return rnb;

}



CsvFormat kgmod::kgGolap::nodeimage(
	string traFilter,string itemFilter,
	string factFilter,
	string gTransaction,string gNode,
	string itemVal) 
{

	NodeImageParams nIpara(
		occ->traAtt->traMax , 
		occ->itemAtt->itemMax ,
		factTable->recMax,
		config->traFile.traFld ,
		config->traFile.itemFld
	);

	if(!traFilter.empty()){
		nIpara.traFilter = fil->makeTraBitmap(traFilter);
	}
	if(!itemFilter.empty()){
		nIpara.itemFilter = fil->makeItemBitmap(itemFilter);
	}
	if(!factFilter.empty()){
  	nIpara.factFilter = fil->makeFactBitmap(factFilter);
  }

	if(!gTransaction.empty()){
		vector<string> buf = Cmn::CsvStr::Parse(gTransaction.c_str());
		if (! buf.empty()) {
			nIpara.granularity.first.clear();
			nIpara.granularity.first.reserve(buf.size());
			for (auto& f : buf) {
				if(!config->isinTraGranuFLD(f)){
					string msg = "nodeimage.granularity.transaction(";
					msg += gTransaction;
					msg += ") must be set in config file (traAttFile.granuFields)\n";
					throw kgError(msg);
				}
				nIpara.granularity.first.push_back(f);
			}
		}
	}

	if(!gNode.empty()){
		vector<string> buf = Cmn::CsvStr::Parse(gNode.c_str());
		if (! buf.empty()) {
			nIpara.granularity.second.clear();
			nIpara.granularity.second.reserve(buf.size());
			for (auto& f : buf) {
				if (! occ->itemAtt->isItemAtt(f)) {
					string msg = f;
					msg += " is not item attribute\n";
					throw kgError(msg);
				}
				nIpara.granularity.second.push_back(f);
			}
		}
	}
	if(!itemVal.empty()){
		nIpara.itemVal.reserve(1);
		nIpara.itemVal.push_back(itemVal);
	}
	else {
		string msg = "nodeimage.itemVal must be set in request\n";
		throw kgError(msg);
	}

	Ewah tmpItemBmp;
	Ewah itemBmp;
	mt_occ->itemAtt->bmpList.GetVal(nIpara.granularity.second, nIpara.itemVal, tmpItemBmp);
	tmpItemBmp = tmpItemBmp & nIpara.itemFilter;
	mt_occ->filterItemBmpByTraBmp(tmpItemBmp, nIpara.traFilter, itemBmp);

	CsvFormat imageList(1);
	mt_occ->itemAtt->getImageList(itemBmp, imageList);

	return imageList;

}
void kgmod::kgGolap::saveFilters(QueryParams& query) {
    ofstream traFile(mt_config->outDir + "/trafilter.csv", ios::out);
    traFile << mt_config->traFile.traFld << "\n";
    for (auto i = query.traFilter.begin(), ei = query.traFilter.end(); i != ei; i++) {
        traFile << mt_occ->traAtt->tra[*i] << "\n";
    }
    traFile.close();
    
    ofstream itemFile(mt_config->outDir + "/itemfilter.csv", ios::out);
    itemFile << mt_config->traFile.itemFld << "\n";
    for (auto i = query.itemFilter.begin(), ei = query.itemFilter.end(); i != ei; i++) {
        itemFile << mt_occ->itemAtt->item[*i] << "\n";
    }
    itemFile.close();
    ofstream factFile(mt_config->outDir + "/factfilter.csv", ios::out);
    factFile << "recNo,";
    factFile << mt_config->traFile.traFld << ",";
    factFile << mt_config->traFile.itemFld << "\n";
    for (auto i = query.factFilter.begin(), ei = query.factFilter.end(); i != ei; i++) {
        size_t traNo, itemNo;
        mt_factTable->recNo2keys(*i, traNo, itemNo);
        factFile << *i << ",";
        factFile << mt_occ->traAtt->tra[traNo] << ",";
        factFile << mt_occ->itemAtt->item[itemNo] << "\n";
    }
    factFile.close();

}
int kgmod::kgGolap::prerun() {

	//_env = &_lenv;
	// setArgs();
	config = new Config(opt_inf);
	config->dump(opt_debug);
	mt_config = config;         // マルチスレッド用反則技
      
	//occ = new Occ(config, _env);
	occ = new Occ(config, &_lenv);
	occ->load();
	occ->dump(opt_debug);
	mt_occ = occ;               // マルチスレッド用反則技
        
	//factTable = new FactTable(config, _env, occ);
	factTable = new FactTable(config, &_lenv, occ);

	factTable->load();
	mt_factTable = factTable;   // マルチスレッド用反則技
        
	cmdcache = new cmdCache(config, _env, false);
	cmdcache->dump(opt_debug);

	fil = new Filter(occ,factTable, cmdcache, config, _env, opt_debug);

	return 0;
}
