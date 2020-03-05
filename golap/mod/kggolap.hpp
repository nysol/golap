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

#ifndef mkgolap_h
#define mkgolap_h

#include <pthread.h>
#include <map>
#include <boost/thread.hpp>
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
#include "http.hpp"
#include "request.hpp"
#include "thread.hpp"
#include "facttable.hpp"

using namespace std;
using namespace kglib;

namespace kgmod {
    typedef btree::btree_multimap<float, string> Result;
    
    static pthread_t pt;
    static bool isTimeOut;
    static bool isWaiting = false;
    static void* timerHandle(void* timer) {
        static unsigned int tt = *(unsigned int*)timer;
        if (tt != 0) {
            sleep(tt);
            isTimeOut = true;
            cerr << "time out" << endl;
        }
        return (void*)NULL;
    }
    static void setTimer(unsigned int& timerInSec) {
        if (timerInSec == 0) {
            cerr << "setTimer: infinite" << endl;
            isWaiting = false;
        } else {
            cerr << "setTimer: " << timerInSec << " sec" << endl;
            isTimeOut = false;
            pthread_create(&pt, NULL, &timerHandle, &timerInSec);
            isWaiting = true;
        }
    }
    static void cancelTimer(void) {
        if (! isWaiting) return;
        if (! isTimeOut) {
            pthread_cancel(pt);
            cerr << "timer canceled" << endl;
        }
        isWaiting = false;
    }
    
    static Config* mt_config;
    static Occ* mt_occ;
    static FactTable* mt_factTable;
    Result Enum(Query& query, Ewah& dimBmp, map<string, pair<string, size_t>>& isolatedNodes);
    typedef MtQueue<pair<string, Ewah*>> mq_t;
    void MT_Enum(mq_t* mq, Query* query, map<string, Result>* res);

    //
    // kgGolap 定義
    class kgGolap : public kgMod {
    private:
        string opt_inf;
        
    public:
        bool opt_debug = false;
        Config* config = NULL;
        Occ* occ = NULL;
        FactTable* factTable = NULL;
        Filter* fil = NULL;
        cmdCache* cmdcache;
        
    private:
        void setArgs(void);
        
    public:
        kgGolap(void);
        ~kgGolap(void);
        
        Dimension makeDimBitmap(string& cmdline);
        size_t sizeOfResult(Result res) {
            size_t out = 0;
            for (auto i = res.begin(), ie = res.end(); i != ie; i++) {
                out += i->second.size();
            }
            return out;
        }
//        Result Enum(Query& query, Ewah& DimBmp);
        void Output(Result& res);
        int run(void);
    };
    
    //
    // exec 定義
    class exec : public Http {
        kgGolap* golap_;
        u_short port;
        bool closing_;
        
    public:
        exec(kgGolap* golap, asio::io_service& io_service, u_short port)
        : golap_(golap), Http(io_service, port), port(port), closing_(false) {};
        
        bool isClosing(void) {return closing_;}
        
    private:
        void doControl(EtcReq& etcReq);
        void doRetrieve(EtcReq& etcReq);
        void setQueryDefault(Query& query);
        void co_occurrence(Query& query, map<string, Result>& res);
        void nodestat(NodeStat& nodestat, map<string, Result>& res);
        void nodeimage(NodeImage& nodeimage, map<string, Result>& res);
        void axisValsList(axis_t& flds, vector<vector<pivAtt_t>>& valsList);
        void combiAtt(vector<vector<pivAtt_t>>& valsList, vector<vector<pivAtt_t>>& hdr, vector<pivAtt_t> tmp);
        void worksheet(WorkSheet& worksheet, map<string, Result>& res);
        void pivot(Pivot& pivot, map<string, Result>& res);
        void saveFilters(Query& query);
        void co_occrence_mcmd(Query& query);
        void diff_res_vs_mcmd(void);
        void proc(void) override;
    };
}

#endif /* kggolap_h */
