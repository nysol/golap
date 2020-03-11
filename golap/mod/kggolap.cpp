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
#include "filter.hpp"
#include "kggolap.hpp"

//#include "http.hpp"

using namespace std;
using namespace kgmod;

void* timerLHandle(void* timer) {
	timChkT *tt = (timChkT*)timer;
	sleep(tt->timerInSec);
	*(tt->isTimeOut) = 1;
	cerr << "time out" << endl;
	return (void*)NULL;
}

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

// -----------------------------------------------------------------------------
// 引数の設定
// -----------------------------------------------------------------------------
Result kgmod::kgGolap::Enum( QueryParams& query, Ewah& dimBmp ,size_t tlimit=45) 
{

	map<string, pair<string, size_t>> isolatedNodes;
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

	const char *headstr[] = {
		"node1","node2","frequency","frequency1","frequency2","total",
		"support","confidence","lift","jaccard","PMI","node1n","node2n",""
	};
	Result res(13,headstr);

	cerr << "enumerating" << endl;
	size_t hit = 0;
	bool notSort = false;
	signed int stat = 0;
	unordered_map<size_t, size_t> itemFreq;
    
	Ewah tarTraBmp = query.traFilter & dimBmp & _occ->getliveTra();
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

	Ewah traBmpInFact;
	Ewah itemBmpInFact;

	_factTable->toTraItemBmp(query.factFilter, query.itemFilter, traBmpInFact, itemBmpInFact);
	cerr << "tra in fact "; Cmn::CheckEwah(traBmpInFact);

	tarTraBmp = tarTraBmp & traBmpInFact;
	cerr << "tarTraBmp "; Cmn::CheckEwah(tarTraBmp);

	Ewah tarItemBmp = query.itemFilter & itemBmpInFact;
	cerr << "tarItemBmp "; Cmn::CheckEwah(tarItemBmp);


	traNum = tarTraBmp.numberOfOnes();

	// granularityのsizeが1で、かつ、[0]がキーであれば、granularityを指定していない(=false)
	bool isTraGranu  = (query.granularity.first.size() != 1) ||
                        (query.granularity.first[0] != _config->traFile.traFld);
	bool isNodeGranu = (query.granularity.second.size() != 1) ||
                        (query.granularity.second[0] != _config->traFile.itemFld);


	map<vector<string>, size_t> itemNo4node_map;    // [node] -> coitemsをカウントする代表itemNo

	// すでにresにリストされたノードのセット
	set<string> listedNodes;

	//  粒度なし
	if(!isTraGranu && !isNodeGranu){

		// DEBUG
		size_t icnt = tarItemBmp.numberOfOnes();

		int cnt=1;
		for (auto i2 = tarItemBmp.begin(), ei2 = tarItemBmp.end(); i2 != ei2; i2++) {
			// CHECK 用
			if ( cnt%1000==0 ){ cerr << cnt << "/" << icnt << endl;}
			cnt++;

			if (isTimeOut) {stat = 2; break;}

			string node2 = _occ->getItemCD(*i2);
			vector<string> vnode2; vnode2.resize(1);
			vnode2[0] = node2;


			// *i2の持つtransaction
			Ewah traBmp;
			// _occ->bmpList.GetVal(_config->traFile.itemFld, _occ->itemAtt_item(*i2), traBmp);
			_occ->getTraBmpFromItem(*i2,traBmp);
			traBmp = traBmp & tarTraBmp;

			// これいる？
			Ewah *itemBmp=NULL;
			if(! _occ->ibmpList_GetVal(query.granularity.second,vnode2,itemBmp)){
					itemFreq[*i2]  = 0;
			}
			else{
				size_t cnt = 0;
				for (auto t = traBmp.begin(), et = traBmp.end(); t != et; t++) {
					if (!_factTable->existInFact(*t, *i2, query.factFilter)) continue;
					cnt++;
				}
				itemFreq[*i2]  = cnt;
			}

			if ( ((float)itemFreq[*i2]/traNum) < query.selCond.minSup ){ continue; }
			if (itemFreq[*i2] == 0) continue;

			// i2 が繋がってる nodeid ごとの件数(頻度)	 
			map<size_t, size_t> coitems;

			// sup条件を満たしている場合、もう一回loopしてpairカウント
			for (auto t2 = traBmp.begin(), et2 = traBmp.end(); t2 != et2; t2++) {

				if (isTimeOut) {stat = 2; break;}
				
				if (!_factTable->existInFact(*t2, *i2, query.factFilter)) continue;
				
				Ewah item_i1 = _occ->getItmBmpFromTra(*t2) & tarItemBmp;

				for (auto i1 = item_i1.begin(), ei1 = item_i1.end(); i1 != ei1; i1++) {
					if (isTimeOut) {stat = 2; break;}
					if (*i1 >= *i2) break;
          if (!_factTable->existInFact(*t2, *i1, query.factFilter)) { 
          	continue;
          }
					coitems[*i1]++;
				}
			}

			//++++++++++++++
			const string delim = ":";
			string item2;
			string itemName2;
			for (auto& f : query.granularity.second) {
				if (f == _config->traFile.itemFld) {
					item2 += _occ->getItemCD(*i2) + delim;
					itemName2 += _occ->getItemName(*i2) + delim;
				} else {
					item2 += _occ->getItemCD(*i2, f) + delim;
					itemName2 += _occ->getItemName(*i2, f) + delim;
				}
			}
			Cmn::EraseLastChar(item2);
			Cmn::EraseLastChar(itemName2);
			isolatedNodes[item2] = make_pair(itemName2, itemFreq[*i2]);

			for (auto i1 = coitems.begin(), ei1 = coitems.end(); i1 != ei1; i1++) {
				if (isTimeOut) {stat = 2; break;}
				string item1 ;
				string itemName1;
				for (auto& f : query.granularity.second) {

					if (f == _config->traFile.itemFld) {
						item1 += _occ->getItemCD(i1->first) + delim;
						itemName1 += _occ->getItemName(i1->first) + delim;
						//item2 += _occ->getItemCD(*i2) + delim;
						//itemName2 += _occ->getItemName(*i2) + delim;
					}
					else {
						item1 += _occ->getItemCD(i1->first, f) + delim;
						itemName1 += _occ->getItemName(i1->first, f) + delim;
						//item2 += _occ->getItemCD(*i2, f) + delim;
						//itemName2 += _occ->getItemName(*i2, f) + delim;
					}
				}
				Cmn::EraseLastChar(item1);
				Cmn::EraseLastChar(itemName1);
				//Cmn::EraseLastChar(item2);
				//Cmn::EraseLastChar(itemName2);

				size_t freq = i1->second;
				float sup = (float)freq / traNum;
				if (sup < query.selCond.minSup) continue;

				float conf1 = (float)freq / itemFreq[i1->first];
				if (conf1 < query.selCond.minConf) continue;

				float jac = (float)freq / (itemFreq[i1->first] + itemFreq[*i2] - freq);
				if (jac < query.selCond.minJac) continue;
				
				double lift = (double)(freq * traNum) / (itemFreq[i1->first] * itemFreq[*i2]);
				if (lift < query.selCond.minLift) continue;

				float pmi = Cmn::calcPmi(freq, itemFreq[i1->first], itemFreq[*i2], traNum);
				if (pmi < query.selCond.minPMI) continue;

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

				res.insert(make_pair(skey, dt));
				listedNodes.insert(item1);
				listedNodes.insert(item2);
				auto it1 = isolatedNodes.find(item1);
				if (it1 != isolatedNodes.end()) isolatedNodes.erase(it1);
				auto it2 = isolatedNodes.find(item2);
				if (it2 != isolatedNodes.end()) isolatedNodes.erase(it2);

				if (res.size() > query.sendMax) {
					res.pop();
					stat = 1;
					if (notSort) break;
				}
				hit++;
			}
		}

		if (query.isolatedNodes) {
			vector<string> dt(3);
			dt[0] = "node";
			dt[1] = "noden";
			dt[2] = "frequency";
			res.isoHeadSet(dt);
			for (auto& n : isolatedNodes) {
				dt[0] = n.first;               // node
				dt[1] = n.second.first;              // nodeName
				dt[2] = to_string(n.second.second);  // frequency
				res.isoDataSet(dt);
			}
		}

	}
	//以下粒度あり
	else{
		// [traNo] -> 当該トランザクションが有するTraAttの値リスト(csv)
		vector<string> tra2key;     

		if (isTraGranu) {
        // vector版countKeyValueは処理が比較的重いので、マルチバリューかどうかで処理を分ける
        // なぜquery.traFilter？
        if (query.granularity.first.size() == 1) {
            traNum = _occ->countKeyValue(query.granularity.first[0], &query.traFilter); 
            _occ->getTra2KeyValue(query.granularity.first[0], tra2key);

        } else {
            traNum = _occ->countKeyValue(query.granularity.first, query.traFilter);
            _occ->getTra2KeyValue(query.granularity.first, tra2key);
        }
    }

    //map<string, map<size_t, Ewah>> ex_occ_cacheOnceQuery;  // ["field name"] -> {traNo, occ(itemBmp)}
    unordered_map<string, Ewah> ex_occ_cacheOnceQuery;      // ["field name"] -> item bitmap

    set<vector<string>> checked_node2;       // [vnodes] -> exists

		// DEBUG		
		size_t icnt =query.itemFilter.numberOfOnes();
		int cnt=1;
		
		cerr << "icnt "<< tarItemBmp.numberOfOnes() << endl;
		
		// 件数のみカウント なくてもできる？
    for (auto i2 = tarItemBmp.begin(), ei2 = tarItemBmp.end(); i2 != ei2; i2++) {

			if (isTimeOut) {stat = 2; break;}
			vector<string> vnode2;
			string node2;
			if (isNodeGranu) {
				// itemNo(2)から指定した粒度のキー(node2)を作成する。
				vnode2 = _occ->getItemCD(*i2, query.granularity.second);
				node2 = Cmn::CsvStr::Join(vnode2, ":");
			} else {
				node2 = _occ->getItemCD(*i2);
				vnode2.resize(1);
				vnode2[0] = node2;
			}

			if (itemFreq.find(*i2) != itemFreq.end()) { continue; }
					
			if (checked_node2.find(vnode2) != checked_node2.end()) { continue; }

			if (isTraGranu) {
					itemFreq[*i2] = _factTable->attFreq(query.granularity.second, vnode2,
                                        tarTraBmp,tarItemBmp,query.factFilter, tra2key);        
        
      }else{
      
				itemFreq[*i2]   = _factTable->attFreq(query.granularity.second, vnode2,
                                          tarTraBmp,tarItemBmp,query.factFilter);
			}

			checked_node2.insert(vnode2);

			Ewah itemInTheAtt2 = _occ->getItmBmpFromGranu(query.granularity.second, vnode2);
			itemInTheAtt2 = itemInTheAtt2  & tarItemBmp;

			if (isNodeGranu) {
				// これの速度確認要
				for (auto at2 = itemInTheAtt2.begin(), eat2 = itemInTheAtt2.end(); at2 != eat2; at2++) {
					itemFreq[*at2] = itemFreq[*i2];
				}
			}
		}

		unordered_map<vector<string>, bool, boost::hash<vector<string>>> checked_node2_b;
		for (auto i2 = tarItemBmp.begin(), ei2 = tarItemBmp.end(); i2 != ei2; i2++) {
			// CHECK 用
			if ( cnt%1000==0 ){ cerr << cnt << "/" << icnt << endl;}
			cnt++;
			if (isTimeOut) {stat = 2; break;}

	    vector<string> vnode2;
  	  string node2;

    	if (isNodeGranu) {
    		// itemNo(2)から指定した粒度のキー(node2)を作成する。
      	vnode2 = _occ->getItemCD(*i2, query.granularity.second);
				node2 = Cmn::CsvStr::Join(vnode2, ":");
			} else {
				node2 = _occ->getItemCD(*i2);
				vnode2.resize(1);
				vnode2[0] = node2;
			}

			if (checked_node2_b.find(vnode2) != checked_node2_b.end()) {
				continue;
			}

			checked_node2_b[vnode2]= true;


			//++++++++++
			// 個数でした方がいい？
			if (  ((float)itemFreq[*i2]/traNum) < query.selCond.minSup ){
				continue;
			}
			if (itemFreq[*i2] == 0) continue;

			Ewah itemInTheAtt2;
			itemInTheAtt2 = _occ->getItmBmpFromGranu(query.granularity.second, vnode2);
			itemInTheAtt2 = itemInTheAtt2 & tarItemBmp;

			map<size_t, size_t> coitems;


			set<pair<string, string>> checked_node1;    
			unordered_map<pair<size_t, string>, bool, boost::hash<pair<size_t, string>>> checked_item1;
	    unordered_map<vector<string>, size_t, boost::hash<vector<string>>> itemNo4node_map;

			Ewah tra_i2_tmp1;
			for (auto at2 = itemInTheAtt2.begin(), eat2 = itemInTheAtt2.end(); at2 != eat2; at2++) {
				if (isTimeOut) {stat = 2; break;}
				Ewah tra_i2_tmp2; 
				_occ->getTraBmpFromItem(*at2,tra_i2_tmp2);
				tra_i2_tmp1 = tra_i2_tmp1 | tra_i2_tmp2;
			}

			// item2 の持つtra LIST //多分この辺でfactfilterとのチェックした方がいいはず
			Ewah tra_i2 = tra_i2_tmp1 & tarTraBmp;

			// ここおそくなりそうbmpで一気する方法はある？ 
			unordered_map<vector<string>, bool , boost::hash<vector<string>>> checked_tra1;

			for (auto t2 = tra_i2.begin(), et2 = tra_i2.end(); t2 != et2; t2++) {

				if (isTimeOut) {stat = 2; break;}

				vector<string> vTraAtt;
				_occ->traNo2traAtt(*t2, query.granularity.first, vTraAtt);
				string traAtt = Cmn::CsvStr::Join(vTraAtt, ":");

				if (checked_tra1.find(vTraAtt) != checked_tra1.end()) { continue; }

				checked_tra1[vTraAtt] = true;

				// queryのfactFilterに，traNo(*t2)と拡張済itemNo(*at2)の組み合わせがなければcontinue
				if(isTraGranu){
					if (!_factTable->existInFact(*t2, itemInTheAtt2 ,query.granularity.first ,vTraAtt,tarTraBmp, query.factFilter,query.granularity.second)) continue;
				}
				else{
					if (!_factTable->existInFact(*t2, itemInTheAtt2 , query.factFilter)) continue;
				}

	  	  Ewah item_i1;
				// tra拡張
				if (isTraGranu) {
					// t2の属性から、requestで指定された粒度で対象トランザクションを拡張した上で、対象itemNo(1)を拡張する
					// 拡張する際にfact table適応させる？
          _occ->expandItemByGranu(*t2, query.granularity.first, tarTraBmp,
                                    tarItemBmp, item_i1, ex_occ_cacheOnceQuery);
				}
				else{
					// t2の持つitem LIST
					item_i1 = _occ->getItmBmpFromTra(*t2) & tarItemBmp;
	  	  }

				for (auto i1 = item_i1.begin(), ei1 = item_i1.end(); i1 != ei1; i1++) {
  	    	if (isTimeOut) {stat = 2; break;}

					if(isNodeGranu){
		        vector<string> vnode1 = _occ->getItemCD(*i1, query.granularity.second);
  		      string node1 = Cmn::CsvStr::Join(vnode1, ":");
  	  	    if (checked_node1.find({node1, traAtt}) == checked_node1.end()) {
      			  checked_node1.insert({node1, traAtt});
						} else {
							continue;
						}
						Ewah itemInTheAtt1 = _occ->getItmBmpFromGranu(query.granularity.second, vnode1);
						itemInTheAtt1 = itemInTheAtt1  & tarItemBmp;
		        if (*(itemInTheAtt1.begin()) >= *(itemInTheAtt2.begin())) continue;
						if(isTraGranu){
							if (!_factTable->existInFact(*t2, itemInTheAtt1 ,query.granularity.first ,vTraAtt,tarTraBmp, query.factFilter,query.granularity.second)) continue;
						}
						else{
							if (!_factTable->existInFact(*t2, itemInTheAtt1 , query.factFilter)) continue;
						}

						size_t itemNo4node;
						if (itemNo4node_map.find(vnode1) == itemNo4node_map.end()) {
        			itemNo4node_map[vnode1] = *itemInTheAtt1.begin();
         			itemNo4node = *itemInTheAtt1.begin();
		        } else {
    		      itemNo4node = itemNo4node_map[vnode1];
	     		 }	
						coitems[itemNo4node]++;

					}
					else{//istraGranu
	    	  	if (*i1 >= *i2) break;
						if (!_factTable->existInFact(*t2, *i1 ,query.granularity.first ,vTraAtt,tarTraBmp, query.factFilter,query.granularity.second)) continue;
						coitems[*i1]++;
	    	  }
				}

			}
			const string delim = ":";
			string item2;
			string itemName2;
			for (auto& f : query.granularity.second) {
				if (f == _config->traFile.itemFld) {
					item2 += _occ->getItemCD(*i2) + delim;
					itemName2 += _occ->getItemName(*i2) + delim;
				} else {
					item2 += _occ->getItemCD(*i2, f) + delim;
					itemName2 += _occ->getItemName(*i2, f) + delim;
				}
			}
			Cmn::EraseLastChar(item2);
			Cmn::EraseLastChar(itemName2);
			isolatedNodes[item2] = make_pair(itemName2, itemFreq[*i2]);

        for (auto i1 = coitems.begin(), ei1 = coitems.end(); i1 != ei1; i1++) {
            if (isTimeOut) {stat = 2; break;}
            string item1;
            string itemName1;
            for (auto& f : query.granularity.second) {
                if (f == _config->traFile.itemFld) {
                    item1 += _occ->getItemCD(i1->first) + delim;
                    itemName1 += _occ->getItemName(i1->first) + delim;
                } else {
                    item1 += _occ->getItemCD(i1->first, f) + delim;
                    itemName1 += _occ->getItemName(i1->first ,f) + delim;
                }
            }
            Cmn::EraseLastChar(item1);
            Cmn::EraseLastChar(itemName1);
            
            size_t freq = i1->second;
            float sup = (float)freq / traNum;
            if (sup < query.selCond.minSup) continue;

            float conf1 = (float)freq / itemFreq[i1->first];
            if (conf1 < query.selCond.minConf) continue;
            
            float jac = (float)freq / (itemFreq[i1->first] + itemFreq[*i2] - freq);
            if (jac < query.selCond.minJac) continue;
            double lift = (double)(freq * traNum) / (itemFreq[i1->first] * itemFreq[*i2]);
            if (lift < query.selCond.minLift) continue;
            
            float pmi = Cmn::calcPmi(freq, itemFreq[i1->first], itemFreq[*i2], traNum);
            if (pmi < query.selCond.minPMI) continue;

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
						
						res.insert(make_pair(skey, dt));
            listedNodes.insert(item1);
            listedNodes.insert(item2);
            auto it1 = isolatedNodes.find(item1);
            if (it1 != isolatedNodes.end()) isolatedNodes.erase(it1);
            auto it2 = isolatedNodes.find(item2);
            if (it2 != isolatedNodes.end()) isolatedNodes.erase(it2);

            if (res.size() > query.sendMax) {
            	res.pop();
              stat = 1;
              if (notSort) break;
            }

            hit++;
        }
		}
		if (query.isolatedNodes) {
			vector<string> dt(3);
			dt[0] = "node";
			dt[1] = "noden";
			dt[2] = "frequency";
			res.isoHeadSet(dt);
			for (auto& n : isolatedNodes) {
				dt[0] = n.first;               // node
				dt[1] = n.second.first;              // nodeName
				dt[2] = to_string(n.second.second);  // frequency
				res.isoDataSet(dt);
			}
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

void kgmod::kgGolap::MT_Enum(mq_t* mq, QueryParams* query, map<string, Result>* res ,unsigned int *dl) 
{
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
		string sortKey,string sendMax,string dimension,string deadline,
		string isolatedNodes
) 
{

	QueryParams qPara(
		_occ->traMax() , 
		_occ->itemMax() ,
		_factTable->recMax,
		_config->sendMax,
		_config->traFile.traFld ,
		_config->traFile.itemFld
	);

	if(!traFilter.empty()){
		qPara.traFilter = _fil->makeTraBitmap(traFilter);
	}
	if(!itemFilter.empty()){
		qPara.itemFilter = _fil->makeItemBitmap(itemFilter);
	}
	if(!factFilter.empty()){
		qPara.factFilter = _fil->makeFactBitmap(factFilter);
	}

	if(!gTransaction.empty()){
		vector<string> buf = Cmn::CsvStr::Parse(gTransaction.c_str());
		if (! buf.empty()) {
			qPara.granularity.first.clear();
			qPara.granularity.first.reserve(buf.size());
			for (auto& f : buf) {
				if(!_config->isinTraGranuFLD(f)){
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
				if (! _occ->isItemAtt(f)) {
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
			qPara.dimension.DimBmpList[param[i]] = _occ->getTraBmp(qPara.dimension.key, param[i]);
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

	unsigned int dline = _config->deadlineTimer;
	if(! deadline.empty()){
		dline = atol(deadline.c_str());
	}
	if(! isolatedNodes.empty()){
		if ( boost::iequals(isolatedNodes , "true")){
			qPara.isolatedNodes= true;
		}
	}

	map<string, Result> res;

	if (qPara.dimension.DimBmpList.size() == 0) {	
		// ダミーで_occ->liveTraをセットしてる？
		Ewah dmy = _occ->getliveTra();
		res[""] = Enum(qPara, dmy,dline); 
	}
	else{
		if (_config->mt_enable) {
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
			for (int i = 0; i < _config->mt_degree; i++) {
				thg.push_back(
					boost::thread([this,&mq, &qPara, &res,&dline] {MT_Enum(&mq, &qPara, &res ,&dline);})
				);
			}
			for (boost::thread& th : thg) { th.join();}
		}
		else{
			for (auto i = qPara.dimension.DimBmpList.begin(); i != qPara.dimension.DimBmpList.end(); i++) {
				res[i->first] = Enum(qPara, i->second,dline);
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
) 
{

	NodeStatParams nSpara(
		_occ->traMax() , 
		_occ->itemMax() ,
		_factTable->recMax,
		_config->traFile.traFld ,
		_config->traFile.itemFld
	);

	if(!traFilter.empty()){
		nSpara.traFilter = _fil->makeTraBitmap(traFilter);
	}
	if(!itemFilter.empty()){
		nSpara.itemFilter = _fil->makeItemBitmap(itemFilter);
	}
	if(!factFilter.empty()){
		nSpara.factFilter = _fil->makeFactBitmap(factFilter);
	}

	if(!gTransaction.empty()){
		vector<string> buf = Cmn::CsvStr::Parse(gTransaction.c_str());
		if (! buf.empty()) {
			nSpara.granularity.first.clear();
			nSpara.granularity.first.reserve(buf.size());
			for (auto& f : buf) {
				if(!_config->isinTraGranuFLD(f)){
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
				if (! _occ->isItemAtt(f)) {
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
			nSpara.vals[i].first.setValPosMap(_factTable->valPosMap());
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
	//signed int stat = 0;
	vector< vector<string> > rnb;
	vector<string> header;
	header.push_back(Cmn::CsvStr::Join(nSpara.granularity.second, ":")) ;
	for (auto& v : nSpara.vals) {
		header.push_back(v.second);
	}
	rnb.push_back(header);
	string traHeader = "";

	//Ewah itemBmp;
	//_occ->itemAtt->bmpList.GetVal(nSpara.granularity.second, nSpara.itemVal, itemBmp);

	Ewah itemBmp =  _occ->getItmBmpFromGranu(nSpara.granularity.second,nSpara.itemVal);
	itemBmp = itemBmp & nSpara.itemFilter;


	string itemValj = Cmn::CsvStr::Join(nSpara.itemVal, ":");
	_factTable->aggregate(
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
		_occ->traMax() , 
		_occ->itemMax() ,
		_factTable->recMax,
		_config->traFile.traFld ,
		_config->traFile.itemFld
	);

	if(!traFilter.empty()){
		nIpara.traFilter = _fil->makeTraBitmap(traFilter);
	}
	if(!itemFilter.empty()){
		nIpara.itemFilter = _fil->makeItemBitmap(itemFilter);
	}
	if(!factFilter.empty()){
		nIpara.factFilter = _fil->makeFactBitmap(factFilter);
	}

	if(!gTransaction.empty()){
		vector<string> buf = Cmn::CsvStr::Parse(gTransaction.c_str());
		if (! buf.empty()) {
			nIpara.granularity.first.clear();
			nIpara.granularity.first.reserve(buf.size());
			for (auto& f : buf) {
				if(!_config->isinTraGranuFLD(f)){
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
				if (! _occ->isItemAtt(f)) {
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

	Ewah itemBmp;
	//_occ->itemAtt->bmpList.GetVal(nIpara.granularity.second, nIpara.itemVal, tmpItemBmp);
	Ewah tmpItemBmp = _occ-> getItmBmpFromGranu(nIpara.granularity.second, nIpara.itemVal);

	tmpItemBmp = tmpItemBmp & nIpara.itemFilter;
	_occ->filterItemBmpByTraBmp(tmpItemBmp, nIpara.traFilter, itemBmp);

	CsvFormat imageList(1);
	_occ->getImageList(itemBmp, imageList);

	return imageList;

}

void kgmod::kgGolap::saveFilters(QueryParams& query) 
{

	ofstream traFile(_config->outDir + "/trafilter.csv", ios::out);
	traFile << _config->traFile.traFld << "\n";
	for (auto i = query.traFilter.begin(), ei = query.traFilter.end(); i != ei; i++) {
		traFile << _occ->getTraCD(*i) << "\n";
	}
	traFile.close();

	ofstream itemFile(_config->outDir + "/itemfilter.csv", ios::out);
	itemFile << _config->traFile.itemFld << "\n";
	for (auto i = query.itemFilter.begin(), ei = query.itemFilter.end(); i != ei; i++) {
		itemFile << _occ->getItemCD(*i) << "\n";
	}
	itemFile.close();

	ofstream factFile(_config->outDir + "/factfilter.csv", ios::out);
	factFile << "recNo,";
	factFile << _config->traFile.traFld << ",";
	factFile << _config->traFile.itemFld << "\n";
	for (auto i = query.factFilter.begin(), ei = query.factFilter.end(); i != ei; i++) {
		size_t traNo, itemNo;
		_factTable->recNo2keys(*i, traNo, itemNo);
		factFile << *i << ",";
		factFile << _occ->getTraCD(traNo) << ",";
		factFile << _occ->getItemCD(itemNo) << "\n";
	}
	factFile.close();
}

int kgmod::kgGolap::prerun() 
{

	_config = new Config(opt_inf);
	_config->dump(opt_debug);

	_occ = new Occ(_config, &_lenv);
	_occ->load();
	_occ->dump(opt_debug);

	_factTable = new FactTable(_config, &_lenv, _occ);
	_factTable->load();

	cmdcache = new cmdCache(_config, false);
	cmdcache->dump(opt_debug);

	_fil = new Filter(_occ,_factTable, cmdcache, _config, _env, opt_debug);

	return 0;
}

//#include "cmdcache.hpp"
	///mt_config = config; 
	// マルチスレッド用反則技
	//mt_occ = occ; 
	//mt_factTable = factTable;   // マルチスレッド用反則技
