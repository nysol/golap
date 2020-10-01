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
#include <limits>
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
	tt->isTimeOut = 1;
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
/*
pthread_t kgmod::kgGolap::get_thID(){
	return pthread_self();
}
void kgmod::kgGolap::can_thID(pthread_t id){
	//int eid = pthread_cancel(id);
	//cerr << "teid " << eid << endl;
	pthread_exit(NULL);
}
*/

// -----------------------------------------------------------------------------
// 引数の設定
// -----------------------------------------------------------------------------
void kgmod::kgGolap::calcDiffData_granu(pair<string, string>& item, QueryParams& query,
										Ewah& tarTraBmp, Ewah& tarItemBmp, Result& res,
										map<string, Ewah>& traBmpCache, vector<string>& dt) {
	bool isTraGranu  = (query.granularity.first.size() != 1) ||
                    	(query.granularity.first[0] != _config->traFile.traFld);

	vector<string> itemCD(2);
	itemCD[0] = item.first;
	itemCD[1] = item.second;
	vector<vector<string>> vnode(2);
	vnode[0] = Cmn::Split(item.first, ':');
	vnode[1] = Cmn::Split(item.second, ':');
	vector<Ewah> itemBmp(2);
	itemBmp[0] = _occ->getItmBmpFromGranu(query.granularity.second, vnode[0]);
	itemBmp[1] = _occ->getItmBmpFromGranu(query.granularity.second, vnode[1]);
	itemBmp[0] = itemBmp[0] & tarItemBmp;
	itemBmp[1] = itemBmp[1] & tarItemBmp;

	vector<Ewah> traBmpInFact(2);
	vector<size_t> iFreq(2);

	size_t traNum = res.getDiffCache_traNum();
	size_t freq;
	boost::optional<size_t> cf = res.getDiffCache_corrFreq(item.first, item.second);
	vector<boost::optional<size_t>> f(2);
	for (size_t i = 0; i < 2; i++) {
	 	f[i] = res.getDiffCache_freq(itemCD[i]);
	}

	if (cf && f[0] && f[1]) {
		freq = *cf;
		for (size_t i = 0; i < 2; i++) {
			iFreq[i] = *f[i];
		}
	} else {
		for (size_t i = 0; i < 2; i++) {
			auto ct = traBmpCache.find(itemCD[i]);
			if ( ct == traBmpCache.end()) {
				Ewah tmp_traBmp;
				unordered_map<vector<string>, bool , boost::hash<vector<string>>> checked_tra;
				iFreq[i] = 0;
				// Ewah traBmp;
				for (auto at2 = itemBmp[i].begin(), eat2 = itemBmp[i].end(); at2 != eat2; at2++) {
					Ewah tra_i2_tmp2;
					_occ->getTraBmpFromItem(*at2,tra_i2_tmp2);
					tmp_traBmp = tmp_traBmp | tra_i2_tmp2;
				}
				tmp_traBmp = tmp_traBmp & tarTraBmp;

				if (isTraGranu) {
					for (auto it = tmp_traBmp.begin(), eit = tmp_traBmp.end(); it != eit; it++) {
						vector<string> vTraAtt;
						_occ->traNo2traAtt(*it, query.granularity.first, vTraAtt);
						if (checked_tra.find(vTraAtt) != checked_tra.end()) continue;

						if (!_factTable->existInFact(*it, itemBmp[i] , query.factFilter)) continue;

						Ewah traInTheAtt = _occ->getTraBmpFromGranu(query.granularity.first, vTraAtt);
						traInTheAtt = traInTheAtt  & tarTraBmp;
						if (traInTheAtt.numberOfOnes() == 0) continue;
						Ewah oneBit;
						oneBit.set(*(traInTheAtt.begin()));
						traBmpInFact[i] = traBmpInFact[i] | oneBit;

						checked_tra[vTraAtt]=true;
					}
				} else {
					for (auto it = tmp_traBmp.begin(), eit = tmp_traBmp.end(); it != eit; it++) {
						if (!_factTable->existInFact(*it, itemBmp[i], query.factFilter)) continue;
						Ewah oneBit;
						oneBit.set(*it);
						traBmpInFact[i] = traBmpInFact[i] | oneBit;
					}
				}

				traBmpCache[itemCD[i]] = traBmpInFact[i];
			} else {
				traBmpInFact[i] = ct->second;
			}

			iFreq[i] = traBmpInFact[i].numberOfOnes();
		}
		Ewah tmp = traBmpInFact[0] & traBmpInFact[1];
		freq = tmp.numberOfOnes();

		for (size_t i = 0; i < 2; i++) {
			if (!f[i]) res.setDiffCache_freq(itemCD[i], iFreq[i]);
		}

		// to compare results
		// for (size_t i = 0; i < 2; i++) {
		// 	if (f[i]) {
		// 		if (*f[i] == iFreq[i]) {
		// 			cerr << itemCD[i] << " cache matched: " << *f[i] << "," << iFreq[i] << endl;
		// 		} else {
		// 			cerr << itemCD[i] << " cache not match: " << *f[i] << "," << iFreq[i] << endl;
		// 		}
		// 	}
		// }
	}

	float sup = (float)freq / traNum;
	float conf1 = (float)freq / iFreq[0];;
	double lift = (double)(freq * traNum) / (iFreq[0] * iFreq[1]);
	float jac = (float)freq / (iFreq[0] + iFreq[1] - freq);
	float pmi = Cmn::calcPmi(freq, iFreq[0], iFreq[0], traNum);

	dt.resize(13);
	dt[0] = item.first;
	dt[1] = item.second;
	dt[2] = toString(freq);
	dt[3] = toString(iFreq[0]);
	dt[4] = toString(iFreq[1]);
	dt[5] = toString(traNum);
	if (freq != 0) {
		char conv[64];
		if (! std::isnan(sup)) {
			sprintf(conv,"%.5f",sup);   dt[6] = conv;
		}
		if (! std::isnan(conf1)) {
			sprintf(conv,"%.5f",conf1); dt[7] = conv;
		}
		if (! std::isnan(lift)) {
			sprintf(conv,"%.5lf",lift); dt[8] = conv;
		}
		if (! std::isnan(jac)) {
			sprintf(conv,"%.5f",jac);   dt[9] = conv;
		}
		if (! std::isnan(pmi)) {
			sprintf(conv,"%.5f",pmi);   dt[10]= conv;
		}
	}
	// dt[11] = item.first;
	// dt[12] = item.second;
	dt[11] += _occ->getItemName(vnode[0], query.granularity.second);
	dt[12] += _occ->getItemName(vnode[1], query.granularity.second);

	return;
}

void kgmod::kgGolap::calcDiffData_nogranu(pair<string, string>& item, QueryParams& query,
											Ewah& tarTraBmp, Ewah& tarItemBmp, Result& res,
											map<string, Ewah>& traBmpCache, vector<string>& dt) {
	vector<string> itemCD(2);
	itemCD[0] = item.first;
	itemCD[1] = item.second;
	vector<size_t> itemNo(2);
	itemNo[0] = _occ->getItemID(item.first);
	itemNo[1] = _occ->getItemID(item.second);

	vector<Ewah> traBmp(2);
	vector<Ewah> traBmpInFact(2);
	vector<size_t> iFreq(2);

	size_t traNum = res.getDiffCache_traNum();
	size_t freq;
	boost::optional<size_t> cf = res.getDiffCache_corrFreq(item.first, item.second);
	vector<boost::optional<size_t>> f(2);
	for (size_t i = 0; i < 2; i++) {
	 	f[i] = res.getDiffCache_freq(itemCD[i]);
	}

	if (cf && f[0] && f[1]) {
		freq = *cf;
		for (size_t i = 0; i < 2; i++) {
			iFreq[i] = *f[i];
		}
	} else {
		for (size_t i = 0; i < 2; i++) {
			// auto it = cached_bmp.find({i, itemNo[i]});
			auto it = traBmpCache.find(itemCD[i]);
			if (it == traBmpCache.end()) {
				_occ->getTraBmpFromItem(itemNo[i], traBmp[i]);
				traBmp[i] = traBmp[i] & tarTraBmp;
				iFreq[i] = 0;
				for (auto t = traBmp[i].begin(), et = traBmp[i].end(); t != et; t++) {
					if (_factTable->existInFact(*t, itemNo[i], query.factFilter)) {
						iFreq[i]++;
						traBmpInFact[i].set(*t);
					}
				}

				traBmpCache[itemCD[i]] = traBmpInFact[i];
			} else {
				iFreq[i] = it->second.numberOfOnes();
				traBmpInFact[i] = it->second;
			}
		}
		Ewah tmp = traBmpInFact[0] & traBmpInFact[1];
		freq = tmp.numberOfOnes();

		cerr << "****" << itemCD[0] << ":" << itemCD[1] << endl;
		if (!cf) {res.setDiffCache_coorFreq(itemCD[0], itemCD[1], freq);}
		else {cerr << itemCD[0] << ":" << itemCD[1] << endl;}
		for (size_t i = 0; i < 2; i++) {
			if (!f[i]) {res.setDiffCache_freq(itemCD[i], iFreq[i]);}
			else {cerr << itemCD[i] << endl;}
		}
	}

	float sup = (float)freq / traNum;
	float conf1 = (float)freq / iFreq[0];;
	double lift = (double)(freq * traNum) / (iFreq[0] * iFreq[1]);
	float jac = (float)freq / (iFreq[0] + iFreq[1] - freq);
	float pmi = Cmn::calcPmi(freq, iFreq[0], iFreq[0], traNum);

	dt.resize(13);
	dt[0] = item.first;
	dt[1] = item.second;
	dt[2] = toString(freq);
	dt[3] = toString(iFreq[0]);
	dt[4] = toString(iFreq[1]);
	dt[5] = toString(traNum);
	if (freq != 0) {
		char conv[64];
		if (! std::isnan(sup)) {
			sprintf(conv,"%.5f",sup);   dt[6] = conv;
		}
		if (! std::isnan(conf1)) {
			sprintf(conv,"%.5f",conf1); dt[7] = conv;
		}
		if (! std::isnan(lift)) {
			sprintf(conv,"%.5lf",lift); dt[8] = conv;
		}
		if (! std::isnan(jac)) {
			sprintf(conv,"%.5f",jac);   dt[9] = conv;
		}
		if (! std::isnan(pmi)) {
			sprintf(conv,"%.5f",pmi);   dt[10]= conv;
		}
	}
	// dt[11] = item.first;
	// dt[12] = item.second;
	dt[11] = _occ->getItemName(itemNo[0]);	// item.first;
	dt[12] = _occ->getItemName(itemNo[1]); 	// item.second;

	return;
}

void kgmod::kgGolap::calcDiffData(pair<string, string>& item, QueryParams& query, Ewah& tarTraBmp, Ewah& tarItemBmp,
								  Result& res, map<string, Ewah>& traBmpCache, vector<string>& dt) {
	bool isNodeGranu = (query.granularity.second.size() != 1) ||
                        (query.granularity.second[0] != _config->traFile.itemFld);
	bool isTraGranu  = (query.granularity.first.size() != 1) ||
                    	(query.granularity.first[0] != _config->traFile.traFld);

	if (isNodeGranu || isTraGranu) {
		calcDiffData_granu(item, query, tarTraBmp, tarItemBmp, res, traBmpCache, dt);
	} else {
		calcDiffData_nogranu(item, query, tarTraBmp, tarItemBmp, res, traBmpCache, dt);
	}
}

void kgmod::kgGolap::addDiffData(QueryParams& query, map<string, Result>& res) {
	cerr << "adding data for diff" << endl;
	size_t dimNum = res.size();
	if (dimNum < 2) return;

	vector<Ewah> tarTraBmp(dimNum);
	Ewah tarItemBmp;
	Ewah traBmpInFact;
	Ewah itemBmpInFact;

	_factTable->toTraItemBmp(query.factFilter, query.itemFilter, traBmpInFact, itemBmpInFact);
	tarItemBmp = query.itemFilter & itemBmpInFact;

	vector<string> dimValue(dimNum);
	size_t ind = 0;
	for (auto r : res) {
		// cerr << r.first << endl;
		// r.second.dumpDiffCache();
		dimValue[ind] = r.first;
		ind++;
	}

	// vector<size_t> traNum(dimNum);
	for (size_t i = 0; i < dimNum; i++) {
		tarTraBmp[i] = query.traFilter & query.dimension.DimBmpList[dimValue[i]]
						& _occ->getliveTra() & traBmpInFact;
		// traNum[i] = res[dimValue[i]].getDiffCache_traNum();
	}

	typedef set<pair<string, string>> keylist_t;
	vector<keylist_t> keylist(dimNum);
	ind = 0;
	pair<string, string> max_key;
	for (auto& itr : res) {
		// cerr << ind << ":" << itr.first << endl;
		vector<vector<string>> rtn = itr.second.getdata();
		for (auto& data : rtn) {
			pair<string, string> key = make_pair(data[0], data[1]);
			keylist[ind].insert(key);
			if (max_key < key) max_key = key;
		}
		ind++;
	}

	vector<keylist_t::iterator> it(dimNum);
	for (int i = 0; i < dimNum; i++) {
		it[i] = keylist[i].begin();
	}

	vector<map<string, Ewah>> cached_traBmp(dimNum);	// [dimNo][itemCD] -> Ewah(traBmp)
	while (true) {
		pair<string, string> cur_key = max_key;
		bool isFinish = true;
		for (int i = 0; i < dimNum; i++) {
			if (it[i] != keylist[i].end()) {
				isFinish = false;
				if (cur_key > *(it[i])) cur_key = *(it[i]);
			}
		}
		if (isFinish) break;
		// cerr << cur_key.first << "," << cur_key.second << endl;

		ind = 0;
		for (auto& dim : res) {
			// cerr << dim.first << endl;
			if (it[ind] == keylist[ind].end() || *(it[ind]) != cur_key) {
				vector<string> dt(13);
				calcDiffData(cur_key, query, tarTraBmp[ind], tarItemBmp, dim.second, cached_traBmp[ind], dt);
				dim.second.insert(make_pair(FLT_MAX, dt));
				dim.second.incSTSdiffCnt();
			} else {
				it[ind]++;
			}
			ind++;
		}
	}
}


Result kgmod::kgGolap::Enum( QueryParams& query, Ewah& dimBmp ,timChkT *timerST) // ,size_t tlimit=45
{

	map<string, pair<string, size_t>> isolatedNodes;
	// Timer 設置
	//int isTimeOut=0;

	pthread_t pt;
//	timChkT *timerST = new timChkT();
//	timerST->isTimeOut = 0 ;//&isTimeOut;
//	timerST->timerInSec = tlimit;

	// ロック必要用
	//if(!query.runID.empty()){
	//	timerSet.insert(r_tim_t::value_type(query.runID,timerST));
	//}

	if (timerST->timerInSec){ // 要エラーチェック
		cerr << "setTimer: " << timerST->timerInSec << " sec" << endl;
		pthread_create(&pt, NULL, timerLHandle, timerST);
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

	cerr << "qt : " << query.traFilter.numberOfOnes() ;
	cerr << " dimBmp : " << dimBmp.numberOfOnes() ;
	cerr << " liveTra : " << _occ->getliveTra().numberOfOnes() ;
	cerr << " tarTraBmp : " << tarTraBmp.numberOfOnes() ;

	cerr << endl;

	if (tarTraBmp.numberOfOnes() == 0) {
		cerr << "trabitmap is empty" << endl;
		res.setSTS(0,0,0);
		if (timerST->timerInSec){
			if (!timerST->isTimeOut) {
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

	size_t traNum = tarTraBmp.numberOfOnes();
	res.setDiffCache_traNum(traNum);

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

			if (timerST->isTimeOut) {stat = 2; break;}

			string node2 = _occ->getItemCD(*i2);
			vector<string> vnode2; vnode2.resize(1);
			vnode2[0] = node2;


			// *i2の持つtransaction
			Ewah traBmp;
			_occ->getTraBmpFromItem(*i2,traBmp);
			traBmp = traBmp & tarTraBmp;

			size_t lcnt = 0;
			for (auto t = traBmp.begin(), et = traBmp.end(); t != et; t++) {
				if (!_factTable->existInFact(*t, *i2, query.factFilter)) continue;
				lcnt++;
			}
			itemFreq[*i2]  = lcnt;
			res.setDiffCache_freq(node2, lcnt);

			if ( ((float)itemFreq[*i2]/traNum) < query.selCond.minSup ){ continue; }
			if (itemFreq[*i2] == 0) continue;

			// i2 が繋がってる nodeid ごとの件数(頻度)
			map<size_t, size_t> coitems;

			// sup条件を満たしている場合、もう一回loopしてpairカウント
			for (auto t2 = traBmp.begin(), et2 = traBmp.end(); t2 != et2; t2++) {

				if (timerST->isTimeOut) {stat = 2; break;}

				if (!_factTable->existInFact(*t2, *i2, query.factFilter)) continue;

				Ewah item_i1 = _occ->getItmBmpFromTra(*t2) & tarItemBmp;

				for (auto i1 = item_i1.begin(), ei1 = item_i1.end(); i1 != ei1; i1++) {
					if (timerST->isTimeOut) {stat = 2; break;}
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
				if (timerST->isTimeOut) {stat = 2; break;}
				string item1 ;
				string itemName1;
				for (auto& f : query.granularity.second) {

					if (f == _config->traFile.itemFld) {
						item1 += _occ->getItemCD(i1->first) + delim;
						itemName1 += _occ->getItemName(i1->first) + delim;
					}
					else {
						item1 += _occ->getItemCD(i1->first, f) + delim;
						itemName1 += _occ->getItemName(i1->first, f) + delim;
					}
				}
				Cmn::EraseLastChar(item1);
				Cmn::EraseLastChar(itemName1);

				res.setDiffCache_coorFreq(item1, item2, i1->second);

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
		set<vector<string>> checked_node2;
		vector<string> tra2key;
		// Node 粒度を考慮した件数
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
		res.setDiffCache_traNum(traNum);	// diff受渡用

		cerr << "cnt gra start icnt "<< tarItemBmp.numberOfOnes() << endl;
		vector<size_t> xxi = tarItemBmp.toArray();

		//vector<Ewah> item2tra; // 未使用
		vector<size_t> Rep_itemlist;
		//vector<vector<size_t>> tralist; //item => tra
		//unordered_map<size_t, Ewah > Rep_tralist; //tra => item
		vector<Ewah> tralists; //item => tra
		//count
		for (size_t iix = 0 ; iix <  xxi.size(); iix++) {
			size_t i2 = xxi[iix];

			// CHECK 用
			if ( iix%1000==0 ){ cerr << iix << "/" <<xxi.size() << endl;}

			if (timerST->isTimeOut) {stat = 2; break;}

			// 作成チェック入れる
			vector<string> vnode2; string node2;
			if (isNodeGranu) {
				// itemNo(2)から指定した粒度のキー(node2)を作成する。
				vnode2 = _occ->getItemCD(i2, query.granularity.second);
				node2 = Cmn::CsvStr::Join(vnode2, ":");
			} else {
				node2 = _occ->getItemCD(i2);
				vnode2.resize(1);
				vnode2[0] = node2;
			}
			if (checked_node2.find(vnode2) != checked_node2.end()) { continue; }
			checked_node2.insert(vnode2);

			Rep_itemlist.push_back(i2);
			vector<size_t> Rep_tralist0;

			Ewah itemInTheAtt2 = _occ->getItmBmpFromGranu(query.granularity.second, vnode2);
			itemInTheAtt2 = itemInTheAtt2  & (tarItemBmp);
			Ewah tra_i2_tmp1;
			unordered_map<vector<string>, bool , boost::hash<vector<string>>> checked_tra1;

			for (auto at2 = itemInTheAtt2.begin(), eat2 = itemInTheAtt2.end(); at2 != eat2; at2++) {
				if (timerST->isTimeOut) {stat = 2; break;}
				Ewah tra_i2_tmp2;
				_occ->getTraBmpFromItem(*at2,tra_i2_tmp2);
				 tra_i2_tmp1 = tra_i2_tmp1 |tra_i2_tmp2;
			}
			tra_i2_tmp1 = tra_i2_tmp1 & tarTraBmp;

			if (isTraGranu) {
				Ewah tralist0;
				tralist0.reset();
				for (auto i = tra_i2_tmp1.begin(), ei = tra_i2_tmp1.end(); i != ei; i++) {
					vector<string> vTraAtt;
					_occ->traNo2traAtt(*i, query.granularity.first, vTraAtt);
					if (checked_tra1.find(vTraAtt) != checked_tra1.end()) { continue; }

					if (!_factTable->existInFact(*i, itemInTheAtt2 , query.factFilter)) continue;

					Ewah traInTheAtt2 = _occ->getTraBmpFromGranu(query.granularity.first,vTraAtt);
			   	  	traInTheAtt2 = traInTheAtt2  & (tarTraBmp);
			  	   	if ( traInTheAtt2.numberOfOnes()==0 ) {  continue; }
				      Ewah tralist0x;
			        tralist0x.set( *(traInTheAtt2.begin()) );
					tralist0 = tralist0 | tralist0x;

	    		    checked_tra1[vTraAtt]=true;
				}
				//tralist.push_back(Rep_tralist0);
				tralists.push_back(tralist0);
			}
			else{
				Ewah tralist0;
				tralist0.reset();
				for (auto i = tra_i2_tmp1.begin(), ei = tra_i2_tmp1.end(); i != ei; i++) {
					if (!_factTable->existInFact(*i, itemInTheAtt2 , query.factFilter)) continue;
				    Ewah tralist0x;
					tralist0x.set( *(i) );
					tralist0 = tralist0 | tralist0x;
				}

				tralists.push_back(tralist0);
			}
		}

		for(size_t iiv=0 ;iiv <Rep_itemlist.size();iiv++){

			// CHECK 用
			if ( iiv%1000==0 ){ cerr << iiv << "/" << Rep_itemlist.size() << endl;}

			size_t i2no = Rep_itemlist[iiv];

			const string delim = ":";
			string item2;
			string itemName2;
			for (auto& f : query.granularity.second) {
				if (f == _config->traFile.itemFld) {
					item2 += _occ->getItemCD(i2no) + delim;
					itemName2 += _occ->getItemName(i2no) + delim;
				} else {
					item2 += _occ->getItemCD(i2no, f) + delim;
					itemName2 += _occ->getItemName(i2no, f) + delim;
				}
			}
			Cmn::EraseLastChar(item2);
			Cmn::EraseLastChar(itemName2);

			res.setDiffCache_freq(item2, tralists[iiv].numberOfOnes());	// diff受渡用
			if (  ((float)tralists[iiv].numberOfOnes()/traNum) < query.selCond.minSup ){
				continue;
			}

			for(size_t iiv2=iiv+1 ;iiv2 <Rep_itemlist.size();iiv2++){
				size_t i1no = Rep_itemlist[iiv2];
				if (  ((float)tralists[iiv2].numberOfOnes()/traNum) < query.selCond.minSup ){
					continue;
				}
				size_t freq = (tralists[iiv] & tralists[iiv2]).numberOfOnes();
				if(freq==0) continue;

				size_t i1freq = tralists[iiv2].numberOfOnes();
				size_t i2freq = tralists[iiv].numberOfOnes();

				float sup = (float)freq / traNum;
				if (sup < query.selCond.minSup) continue;

				float conf1 = (float)freq / i1freq;
				if (conf1 < query.selCond.minConf) continue;


				float jac = (float)freq / (i1freq + i2freq - freq);
				if (jac < query.selCond.minJac) continue;

				double lift = (double)(freq * traNum) / (i1freq * i2freq);
				if (lift < query.selCond.minLift) continue;

				float pmi = Cmn::calcPmi(freq, i1freq, i2freq, traNum);
				if (pmi < query.selCond.minPMI) continue;

				string item1;
				string itemName1;
				for (auto& f : query.granularity.second) {

					if (f == _config->traFile.itemFld) {
						item1 += _occ->getItemCD(i1no) + delim;
						itemName1 += _occ->getItemName(i1no) + delim;
					}
					else {
						item1 += _occ->getItemCD(i1no, f) + delim;
						itemName1 += _occ->getItemName(i1no, f) + delim;
					}
				}
				Cmn::EraseLastChar(item1);
				Cmn::EraseLastChar(itemName1);

				res.setDiffCache_freq(item1, tralists[iiv2].numberOfOnes());	// diff受渡用
				res.setDiffCache_coorFreq(item1, item2, freq);

				vector<string> dt(13);
				dt[0] = item1;
				dt[1] = item2;
				dt[2] = toString(freq);
				dt[3] = toString(i1freq);
				dt[4] = toString(i2freq);
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
				bool notSort = false;
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
				//listedNodes.insert(item1);
				//listedNodes.insert(item2);
				/*
				auto it1 = isolatedNodes->find(item1);
				if (it1 != isolatedNodes->end()) isolatedNodes->erase(it1);
				auto it2 = isolatedNodes->find(item2);
				if (it2 != isolatedNodes->end()) isolatedNodes->erase(it2);
				*/
				if (res.size() > query.sendMax) {
					res.pop();
					stat = 1;
					if (notSort) break;
				}
				(hit)++;
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

	if (timerST->timerInSec){
		if (timerST->isTimeOut==0 ||timerST->isTimeOut==2 ) {
			pthread_cancel(pt);
			cerr << "timer canceled" << endl;
		}
	}
	//if (!query.runID.empty()){
	//	timerSet.erase(query.runID);
	//}
	//delete timerST;
	//timerST =NULL;
	res.setSTS(stat,res.size(),hit);
	res.showSTS();
	return res;

}

void kgmod::kgGolap::MT_Enum(mq_t* mq, QueryParams* query, map<string, Result>* res ,vector<timChkT *> * timesets)
{
	// ロック必要かも
    mq_t::th_t* T = mq->pop();
    while (T != NULL) {
        cerr << "#" << T->first << ") tarTra:"; Cmn::CheckEwah(T->second.second);
        Result rr = Enum(*query, *(T->second.second),timesets->at(T->first));
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
		string isolatedNodes,string runID
)
{

	QueryParams qPara(
		_occ->traMax() ,
		_occ->itemMax() ,
		_factTable->recMax,
		_config->sendMax,
		_config->traFile.traFld ,
		_config->traFile.itemFld,
		runID
	);

	if(!traFilter.empty()){
		qPara.traFilter = _fil->makeTraBitmap(traFilter);
	}
	if(!itemFilter.empty()){
		qPara.itemFilter = _fil->makeItemBitmap(itemFilter);
	}
	if(!factFilter.empty()){
		_ffilFlag = true;
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
	vector<timChkT *> timesets;
	if (qPara.dimension.DimBmpList.size() == 0) {


		// ダミーで_occ->liveTraをセットしてる？
		timesets.resize(1);
		timesets[0] = new timChkT();
		//	timChkT *timerST = new timChkT();
		timesets[0]->isTimeOut = 0 ;//&isTimeOut;
		timesets[0]->timerInSec = dline;

		if(!runID.empty()){
			timerSet[runID] = timesets;
		}

		Ewah dmy = _occ->getliveTra();
		res[""] = Enum(qPara, dmy,timesets[0]);

		if(!runID.empty()){
			timerSet.erase(runID);
		}

		delete timesets[0];
	}
	else{
		if (_config->mt_enable) {
			mq_t::th_t *th;
			mq_t mq;
			size_t threadNo = 0;

			timesets.resize(qPara.dimension.DimBmpList.size());


			for (auto i = qPara.dimension.DimBmpList.begin(); i != qPara.dimension.DimBmpList.end(); i++) {
				cerr << "running with multi-threading (";
				cerr << threadNo << " of " << qPara.dimension.DimBmpList.size() << ")" << endl;
				th = new mq_t::th_t;
				th->first = threadNo;

				timesets[threadNo] = new timChkT();
				timesets[threadNo]->isTimeOut = 0 ;//&isTimeOut;
				timesets[threadNo]->timerInSec = dline;

/*				Ewah *etmp =  new Ewah
				etmp->expensive_copy(i->second);
				make_tuple(i->first, etmp, i->second);
*/

				th->second.first = i->first;
				th->second.second = new Ewah;
				th->second.second->expensive_copy(i->second);
				mq.push(th);
				threadNo++;
			}
			if(!runID.empty()){
				timerSet[runID] = timesets;
			}
			vector<boost::thread> thg;
			for (int i = 0; i < _config->mt_degree; i++) {
				thg.push_back(
					boost::thread([this,&mq, &qPara, &res,&timesets] {MT_Enum(&mq, &qPara, &res,&timesets );})
				);
			}
			for (boost::thread& th : thg) { th.join();}
			if(!runID.empty()){
				timerSet.erase(runID);
			}
			for (size_t ii = 0; ii != qPara.dimension.DimBmpList.size(); ii++) {
				delete timesets[ii];
			}
		}
		else{
			timesets.resize(qPara.dimension.DimBmpList.size());
			for (size_t ii = 0; ii < qPara.dimension.DimBmpList.size(); ii++) {
				timesets[ii] = new timChkT();
				timesets[ii]->isTimeOut = 0 ;//&isTimeOut;
				timesets[ii]->timerInSec = dline;
			}
			if(!runID.empty()){
				timerSet[runID] = timesets;
			}
			for (auto i = qPara.dimension.DimBmpList.begin(); i != qPara.dimension.DimBmpList.end(); i++) {
				timesets.resize(1);
				timesets[0] = new timChkT();
				timesets[0]->isTimeOut = 0 ;//&isTimeOut;
				timesets[0]->timerInSec = dline;
				if(!runID.empty()){
					timerSet[runID] = timesets;
				}
				res[i->first] = Enum(qPara, i->second,timesets[0]);
			}
			if(!runID.empty()){
				timerSet.erase(runID);
			}
			for (size_t ii = 0; ii < qPara.dimension.DimBmpList.size(); ii++) {
				delete timesets[ii];
			}
		}

		addDiffData(qPara, res);
		for (auto& r : res) {
			r.second.showSTS();
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
		_ffilFlag = true;
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
		_ffilFlag = true;
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
