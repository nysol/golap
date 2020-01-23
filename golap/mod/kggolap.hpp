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
//#include "http.hpp"
//#include "request.hpp"
#include "thread.hpp"
#include "facttable.hpp"
#include "csvformat.hpp"

using namespace std;
using namespace kglib;


namespace kgmod {

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

	// PARAMS STRUCT
	struct NodeImageParams {

		Ewah traFilter;
		Ewah itemFilter;
		// first:transaction granurality, second:node granurality
		pair<vector<string>, vector<string>> granularity;   
		vector<string> itemVal;

		NodeImageParams(size_t traMax,size_t itemMax ,string traFld,string itemFld){

			traFilter.padWithZeroes( traMax + 1 );
			traFilter.inplace_logicalnot();

			itemFilter.padWithZeroes(itemMax + 1);
			itemFilter.inplace_logicalnot();

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
		pair<vector<string>, vector<string>> granularity;   // first:transaction granurality, second:node granurality
		vector<string> itemVal;
    vector<pair<AggrFunc, string>> vals;

		NodeStatParams(size_t traMax,size_t itemMax ,string traFld,string itemFld){

			traFilter.padWithZeroes( traMax + 1 );
			traFilter.inplace_logicalnot();

			itemFilter.padWithZeroes(itemMax + 1);
			itemFilter.inplace_logicalnot();

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
		sel_cond selCond;
		sort_key sortKey;
		size_t sendMax;
		pair<vector<string>, vector<string>> granularity;   // first:transaction granurality, second:node granurality
		Dimension dimension;

		QueryParams(size_t traMax,size_t itemMax ,size_t sndMax ,string traFld,string itemFld){

			traFilter.padWithZeroes( traMax + 1 );
			traFilter.inplace_logicalnot();

			itemFilter.padWithZeroes(itemMax + 1);
			itemFilter.inplace_logicalnot();

			// 分ける必要ある？ 複数指定可？
  		granularity.first.resize(1);
			granularity.first[0] = traFld;

			granularity.second.resize(1);
			granularity.second[0] = itemFld;
			
			sendMax = sndMax;
		
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


//  typedef btree::btree_multimap< float, vector<string> > Result;  
	class Result{

		size_t _fldCnt;
		int _status;
		size_t _sent;
		size_t _hit;
		vector<string> _header;
    typedef btree::btree_multimap< float, vector<string> > Result_t;

		Result_t _body;
		
		
		public:

		Result(size_t size=0):
			_fldCnt(size),_status(0),_hit(0){
			_header.resize(_fldCnt);
		}

		//accessor
		int status(){ return _status;}
		size_t sentCnt(){ return _sent;}
		size_t hitCnt(){ return _hit;}
		size_t fldCnt(){ return _fldCnt;}
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
			cerr << "status:" << _status << ",sent:" << _sent << ",hit:" << _hit << endl;
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

	};

  Result Enum(QueryParams& query, Ewah& dimBmp ,size_t tlimit);
  Result Enum_OLD(QueryParams& query, Ewah& dimBmp ,size_t tlimit);

	struct timChkT{
		unsigned int timerInSec;
		int* isTimeOut;
	};

	static void* timerLHandle(void* timer) {
		timChkT *tt = (timChkT*)timer;
		sleep(tt->timerInSec);
		*(tt->isTimeOut) = 1;
		cerr << "time out" << endl;
		return (void*)NULL;
	}

	//この3つ必要か確認
	static Config* mt_config;
	static Occ* mt_occ;
  static FactTable* mt_factTable;

	typedef MtQueue<pair<string, Ewah*>> mq_t;
	void MT_Enum(mq_t* mq, QueryParams* query, map<string, Result>* res, unsigned int *lim);

	// kgGolap 定義
	class kgGolap {
		private:
			string opt_inf;

		public:
			bool opt_debug = false;
			Config* config = NULL;
			Occ* occ = NULL;
			FactTable* factTable = NULL;
			Filter* fil = NULL;
			cmdCache* cmdcache;
			bool closing_ = false;

		private:
			void setArgs(void);

    public:

			kgEnv   _lenv;    
			kgEnv * _env;

			kgGolap(void);
				kgGolap(char *fn){
				opt_inf = string(fn);
			}
			~kgGolap(void);
        
			Dimension makeDimBitmap(string& cmdline);


      void Output(Result& res);
			int prerun(void);


	    void setQueryDefault(QueryParams& query);        

    	void saveFilters(QueryParams& query);

  		vector<string> getTraAtt(string fldname){
  		  return occ->evalKeyValue(fldname);
  		}

  		vector<string> getItmAtt(string fldname,string ifil=""){
				Ewah itemFilter = fil->makeItemBitmap(ifil);
				return occ->evalKeyValueItem(fldname,&itemFilter);
			}


			vector< vector<string> > nodestat(
				string traFilter,string itemFilter,
				string gTransaction,string gnode,
				string itemVal,string values
			);

			CsvFormat nodeimage(
				string traFilter,string itemFilter,
				string gTransaction,string gnode,
				string itemVal
			);

			map<string, Result> runQuery(
				string traFilter,string itemFilter,
				string gTransaction,string gNode,
				string SelMinSup,string SelMinConf,string SelMinLift,
				string SelMinJac,string SelMinPMI,
				string sortKey,string sendMax,string dimension,string deadline
			);

			void save(){
				occ->exBmpList.save(true);
				occ->ex_occ.save(true);		
			}
  };
}

#endif /* kggolap_h */
