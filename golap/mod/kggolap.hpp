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
#pragma once

#ifndef mkgolap_h
#define mkgolap_h

#include <pthread.h>
#include <map>
#include <boost/thread.hpp>
#include <kgEnv.h>
#include <kgError.h>
#include <kgMethod.h>
#include <kgConfig.h>
#include <kgmod.h>
#include "bidx-ewah.hpp"
#include "occ.hpp"
#include "cmn.hpp"
#include "config.hpp"
#include "btree.hpp"
#include "filter.hpp"
#include "thread.hpp"
#include "facttable.hpp"
#include "csvformat.hpp"

using namespace std;
using namespace kglib;


namespace kgmod {

	// PARAMS STRUCT
	struct NodeImageParams {

		Ewah traFilter;
		Ewah itemFilter;
		Ewah factFilter;
		// first:transaction granurality, second:node granurality
		pair<vector<string>, vector<string>> granularity;
		vector<string> itemVal;

		NodeImageParams(size_t traMax,size_t itemMax ,size_t recMax ,string traFld,string itemFld){

			traFilter.padWithZeroes( traMax + 1 );
			traFilter.inplace_logicalnot();

			itemFilter.padWithZeroes(itemMax + 1);
			itemFilter.inplace_logicalnot();

			factFilter.padWithZeroes( recMax + 1 );
			factFilter.inplace_logicalnot();


			// 分ける必要ある？ 複数指定可？
			granularity.first.resize(1);
			granularity.first[0] = traFld;

			granularity.second.resize(1);
			granularity.second[0] = itemFld;
		}

		void dump(void) {
			cerr << "traFilter: "; Cmn::CheckEwah(traFilter);
			cerr << "itemFilter: "; Cmn::CheckEwah(itemFilter);
			cerr << "granularity(transaction): ";
			for (auto& f : granularity.first) cerr << f << " ";
				cerr << endl;
				cerr << "granularity(node): ";
				for (auto& f : granularity.second) cerr << f << " ";
				cerr << endl;
		}
  };

	struct NodeStatParams {

		Ewah traFilter;
		Ewah itemFilter;
		Ewah factFilter;
		pair<vector<string>, vector<string>> granularity;   // first:transaction granurality, second:node granurality
		vector<string> itemVal;
		vector<pair<AggrFunc, string>> vals;

		NodeStatParams(size_t traMax,size_t itemMax ,size_t recMax ,string traFld,string itemFld){

			traFilter.padWithZeroes( traMax + 1 );
			traFilter.inplace_logicalnot();

			itemFilter.padWithZeroes( itemMax + 1 );
			itemFilter.inplace_logicalnot();

			factFilter.padWithZeroes( recMax + 1 );
			factFilter.inplace_logicalnot();

			// 分ける必要ある？ 複数指定可？
			granularity.first.resize(1);
			granularity.first[0] = traFld;

			granularity.second.resize(1);
			granularity.second[0] = itemFld;
		}

		void dump(void) {
			cerr << "traFilter: "; Cmn::CheckEwah(traFilter);
			cerr << "itemFilter: "; Cmn::CheckEwah(itemFilter);
			cerr << "granularity(transactzwion): ";
			for (auto& f : granularity.first) cerr << f << " ";
				cerr << endl;
				cerr << "granularity(node): ";
				for (auto& f : granularity.second) cerr << f << " ";
				cerr << endl;
		}
	};

	enum sort_key{
		SORT_NONE,
		SORT_SUP,
		SORT_CONF,
		SORT_LIFT,
		SORT_JAC,
		SORT_PMI
	};

	struct Dimension{
		string key;
		map<string, Ewah> DimBmpList;

		Dimension(){
			key = "";
			DimBmpList.clear();
		}

		bool isClear() {
			return DimBmpList.empty();
		}
	};

	struct sel_cond{
		double minSup;
		double minConf;
		double minLift;
		double minJac;
		double minPMI;

		sel_cond():minSup(0),minConf(0),minLift(0),minJac(0),minPMI(-1){}

		void dump(void) {
			cerr << "selCond: " << minSup << " " << minConf << " " << minLift
				<< " " << minJac << " " << minPMI << endl;
    };
	};

	struct QueryParams {

		Ewah traFilter;
		Ewah itemFilter;
		Ewah factFilter;
		sel_cond selCond;
		sort_key sortKey;
		bool isolatedNodes;
		size_t sendMax;
		string runID;
		// first:transaction granurality, second:node granurality
		pair<vector<string>, vector<string>> granularity;
		Dimension dimension;

		QueryParams(size_t traMax,size_t itemMax ,size_t recMax,size_t sndMax ,string traFld,string itemFld,string runid){

			traFilter.padWithZeroes( traMax + 1 );
			traFilter.inplace_logicalnot();

			itemFilter.padWithZeroes(itemMax + 1);
			itemFilter.inplace_logicalnot();

			factFilter.padWithZeroes(recMax + 1);
			factFilter.inplace_logicalnot();

			// 分ける必要ある？ 複数指定可？
			granularity.first.resize(1);
			granularity.first[0] = traFld;

			granularity.second.resize(1);
			granularity.second[0] = itemFld;

			isolatedNodes = false;

			sendMax = sndMax;
			runID = runid;

		}

		void dump(void) {
			cerr << "traFilter; ";  Cmn::CheckEwah(traFilter);
			cerr << "itemFilter; "; Cmn::CheckEwah(itemFilter);
			selCond.dump();
			if (sortKey == SORT_SUP)  cerr << "sortKey: SUP"  << endl;
			else if (sortKey == SORT_CONF) cerr << "sortKey: CONF" << endl;
			else if (sortKey == SORT_LIFT) cerr << "sortKey: LIFT" << endl;
			else if (sortKey == SORT_JAC)  cerr << "sortKey: JAC"  << endl;
			else if (sortKey == SORT_PMI)  cerr << "sortKey: PMI"  << endl;
			cerr << "sendMax: " << sendMax << endl;
			cerr << "granularity(transaction): ";
			for (auto& f : granularity.first) cerr << f << " ";
			cerr << endl;
			cerr << "granularity(node): ";
			for (auto& f : granularity.second) cerr << f << " ";
				cerr << endl;
				if (dimension.key.length() != 0) {
				cerr << "dimension: " << dimension.key << "=";
				for (auto i = dimension.DimBmpList.begin(); i != dimension.DimBmpList.end(); i++) {
					cerr << i->first << " ";
				}
				cerr << endl;
			}
		}
	};



	class Result{

		size_t _fldCnt;
		int _status;
		size_t _sent;
		size_t _hit;
		size_t _diffCnt;
		vector<string> _header;
    	typedef btree::btree_multimap< float, vector<string> > Result_t;
		Result_t _body;
		bool isoFLG;
		vector<string> _isohead;
		vector< vector<string> > _isobody;
		// diff受け渡し用キャッシュ
		struct {
			size_t traNum;				// トランザクション数
			// map<pair<string, string>, size_t> coorFreq;	// 共起数
			// map<string, size_t> freq;	// ノード（アイテム）頻度
		} diffCache;

		public:

		Result(size_t size=0):
			_fldCnt(size),_status(0),_hit(0),_diffCnt(0){
			_header.resize(_fldCnt);
			isoFLG = false;
		}

		Result(size_t size,const char **av ):
			_fldCnt(size),_status(0),_hit(0),_diffCnt(0){
			_header.resize(_fldCnt);
			for (size_t i=0 ; i< _fldCnt ;i++ ){
				_header[i] = av[i];
			}
			isoFLG = false;
		}

		//accessor
		int status(){ return _status;}
		size_t sentCnt(){ return _sent;}
		size_t hitCnt(){ return _hit;}
		size_t fldCnt(){ return _fldCnt;}
		size_t diffCnt(){ return _diffCnt;}
		//char * fldNameCstr(size_t pos){ return _header[pos].c_str();}
		string fldName(size_t pos){ return _header[pos];}


		void setSTS(size_t sts,int sent,int hit){
			_status=sts;
			_sent=sent;
			_hit=hit;
		}
		void setSTS(size_t sts){
			_status=sts;
		}
		void showSTS(){
			cerr << "status:" << _status << ",sent:" << _sent << ",hit:" << _hit << ",diff:" << _diffCnt << endl;
		}
		void incSTSdiffCnt(){
			_diffCnt++;
		}
		string to_s(){
			int cnt=0;
			string rtn = "";
			for (auto j = _body.begin(); j != _body.end(); j++) {
				rtn +=toString(j->second);
				rtn += "\n";
				cnt++;
			}
			return rtn;
		}
		vector < vector<string> > getdata(){
			vector < vector<string> > rtn(_body.size());
			size_t pos=0;
			for (auto j = _body.begin(); j != _body.end(); j++) {
				rtn[pos] = j->second;
				pos++;
			}
			return rtn;
		}

		void setHeader(vector<string>& hed ){
			for (size_t i=0 ; i<hed.size() ;i++ ){
				if(i >= _fldCnt){ break;}
				_header[i] = hed[i];
			}
		}

		void insert( pair<float, vector<string> > pval ){
			_body.insert(pval);
		}
		size_t size(){ return _body.size();}
		void pop(){
			auto pos = _body.end();
		  pos--;
		  _body.erase(pos);
		}
		// vector<string> pop(){
		// 	vector<string> out;
		// 	if (_body.empty()) return out;

		// 	auto pos = _body.end();
		// 	pos--;
		// 	out = pos->second;
		// 	_body.erase(pos);
		// 	return out;
		// }

		// diffCache用
		void setDiffCache_traNum(size_t traNo) {
			diffCache.traNum = traNo;
		}
		size_t getDiffCache_traNum(void) {return diffCache.traNum;}

		// 孤立ノード用
		void isoHeadSet(vector<string>& hed){
			_isohead = hed;
			isoFLG = true;
		}

		void isoDataSet(vector<string>& data){
			_isobody.push_back(data);
		}

		bool isIso(void){ return isoFLG ;}
		size_t isofldCnt(void){ return _isohead.size();}
		string isofldName(size_t i){ return _isohead[i]; }
		vector < vector<string> > getisodata(){
			return _isobody;
		}

		// void cacheFreq(size_t itemNo, size_t freq) {
		// 	auto it = cachedFreq.find(itemNo);
		// 	if (it == cachedFreq.end()) {
		// 		cachedFreq.insert(make_pair(itemNo, freq));
		// 	}
		// }

		// size_t getCachedFreq(string item) {
		// 	auto it = cachedFreq.find(item);
		// 	return it->second;
		// }

		// void dumpCachedFreq(void) {
		// 	cerr << "DumpCachedFreq: " << endl;
		// 	for (auto& i : cacheFreq) {
		// 		cerr << i.first << " : " << i.second << endl;
		// 	}
		// }
	};

	struct timChkT{
		unsigned int timerInSec;
		int isTimeOut;
	};

	typedef MtQueue<pair<string, Ewah*>> mq_t;

	// kgGolap 定義
	class kgGolap {

	private:
		string opt_inf;
		bool opt_debug = false;
		Config* _config = NULL;
		Occ* _occ = NULL;
		FactTable* _factTable = NULL;
		Filter* _fil = NULL;
		cmdCache* cmdcache;
		bool _ffilFlag;

		typedef map< string, vector<timChkT*> > r_tim_t;


		r_tim_t timerSet;

		void setArgs(void);

		Result Enum(QueryParams& query, Ewah& dimBmp ,timChkT *timerST);
		void calcDiffData_nogranu(pair<string, string>& item, QueryParams& query, Ewah& tarTraBmp, Ewah& tarItemBmp,
									Result& res, map<string, Ewah>& traBmpCache, vector<string>& dt);
		void calcDiffData_granu(pair<string, string>& item, QueryParams& query, Ewah& tarTraBmp, Ewah& tarItemBmp,
								Result& res, map<string, Ewah>& traBmpCache, vector<string>& dt);
		void calcDiffData(pair<string, string>& item, QueryParams& query, Ewah& tarTraBmp, Ewah& tarItemBmp,
							Result& res, map<string, Ewah>& traBmpCache, vector<string>& dt);
		void addDiffData(QueryParams& query, map<string, Result>& res);

		void MT_Enum(mq_t* mq, QueryParams* query, map<string, Result>* res,vector<timChkT *> *lim);


    public:


		int timeclear(string s){
			if( timerSet.find(s) != timerSet.end() ){
				for(size_t i=0 ; i < timerSet[s].size();i++){
					timerSet[s][i]->isTimeOut = 2;
				}
				return 0;
			}
			else{
				return 1;
			}
		}

		kgEnv   _lenv;
		kgEnv * _env;

		kgGolap(void);
		kgGolap(char *fn){
			opt_inf = string(fn);
			_ffilFlag = false;
		}

		~kgGolap(void){
			if (_occ != NULL) delete _occ;
			if (_config != NULL) delete _config;
			if (cmdcache!= NULL) delete cmdcache;
		}

      	void Output(Result& res);
		int prerun(void);

    	void saveFilters(QueryParams& query);

  		vector<string> getTraAtt(string fldname){
  		  	return _occ->evalKeyValue(fldname);
  		}

  		vector<string> getItmAtt(string fldname,string ifil=""){
			Ewah itemFilter = _fil->makeItemBitmap(ifil);
			return _occ->evalKeyValueItem(fldname,&itemFilter);
		}


		vector< vector<string> > nodestat(
			string traFilter,string itemFilter,
			string factFilter,
			string gTransaction,string gnode,
			string itemVal,string values
		);

		CsvFormat nodeimage(
			string traFilter,string itemFilter,
			string factFilter,
			string gTransaction,string gnode,
			string itemVal
		);

		map<string, Result> runQuery(
			string traFilter,string itemFilter,
			string factFilter,
			string traFilterFile,string itemFilterFile,
			string factFilterFile,
			string gTransaction,string gNode,
			string SelMinSup,string SelMinConf,string SelMinLift,
			string SelMinJac,string SelMinPMI,
			string sortKey,string sendMax,string dimension,string deadline,
			string isolatedNodes,
			string runID
		);

		void save(){
			_occ->save(true); // もとはexBmpList.のみsave
		};
  	};
}


//#include "http.hpp"
//#include "request.hpp"

	//この3つ必要か確認
	//static Config* mt_config;
	//static Occ* mt_occ;
  //static FactTable* mt_factTable;

	//Result Enum_NEW(QueryParams& query, Ewah& dimBmp ,size_t tlimit);
	//Config* config = NULL;
	//Occ* occ = NULL;
	//FactTable* factTable = NULL;
	//Filter* fil = NULL;

	// 時間計測するなら
	//{
	//chrono::system_clock::time_point timeStart;
	//chrono::system_clock::time_point timeEnd;
	//double elapsedTime;
	//timeStart = chrono::system_clock::now();

	//timeEnd = chrono::system_clock::now();
	//elapsedTime = chrono::duration_cast<chrono::milliseconds>(timeEnd - timeStart).count();
	//cerr << "filter eval time: " << elapsedTime / 1000 << " sec" << endl;
  //}
  // エラー処理
  //	catch(string& msg) {
	//	cerr << msg << endl;
	//	return string(msg);
	//}
	//catch(kgError& err){
	//	auto msg = err.message();
	//	string body = "status:-1\n";
	//	for (auto i = msg.begin(); i != msg.end(); i++) {
	//		body += *i + "\n";
	//	}
	//	return body;
	//}
	//	return "status:-1\n";




#endif /* kggolap_h */
