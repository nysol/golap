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
#include "thread.hpp"

using namespace std;
using namespace kglib;

namespace kgmod {
    typedef struct {
        double minSup;
        double minConf;
        double minLift;
        double minJac;
        double minPMI;
        void dump(void) {cerr << "selCond: " << minSup << " " << minConf << " " << minLift
            << " " << minJac << " " << minPMI << endl;};
    } sel_cond;
    
    typedef enum {SORT_NONE, SORT_SUP, SORT_CONF, SORT_LIFT, SORT_JAC, SORT_PMI} sort_key;
    typedef btree::btree_multimap<float, string> Result;
    typedef struct {string key; map<string, Ewah> DimBmpList;} Dimension;

    typedef struct {
        Ewah traFilter;
        Ewah itemFilter;
        sel_cond selCond;
        sort_key sortKey;
        size_t sendMax;
        pair<string, string> granularity;   // first:transaction granurality, second:node granurality
        Dimension dimension;
        size_t debug_mode;
        void dump(void) {
//            cerr << "traFilter; ";  Cmn::CheckEwah(traFilter);
//            cerr << "itemFilter; "; Cmn::CheckEwah(itemFilter);
            selCond.dump();
            if (sortKey == SORT_SUP)  cerr << "sortKey: SUP"  << endl;
            else if (sortKey == SORT_CONF) cerr << "sortKey: CONF" << endl;
            else if (sortKey == SORT_LIFT) cerr << "sortKey: LIFT" << endl;
            else if (sortKey == SORT_JAC)  cerr << "sortKey: JAC"  << endl;
            else if (sortKey == SORT_PMI)  cerr << "sortKey: PMI"  << endl;
            cerr << "sendMax: " << sendMax << endl;
            cerr << "granularity(transaction): " << granularity.first << endl;
            cerr << "granularity(node): "        << granularity.second << endl;
            if (dimension.key.length() != 0) {
                cerr << "dimension: " << dimension.key << "=";
                for (auto i = dimension.DimBmpList.begin(); i != dimension.DimBmpList.end(); i++) {
                    cerr << i->first << " ";
                }
                cerr << endl;
            }
        }
    } Query;
    typedef struct {
        Ewah traFilter;
        Ewah itemFilter;
        typedef vector<pair<char, string>> axis_t;      // first:[I|T] second:attName
        pair<axis_t, axis_t> axes;                      // first:x-axis second:y-axis
        void dump (void) {
            cerr << "x-axis: ";
            for (auto i : axes.first) {
                cerr << i.first << ":" << i.second << " ";
            }
            cerr << endl;
            cerr << "y-axis: ";
            for (auto i : axes.second) {
                cerr << i.first << ":" << i.second << " ";
            }
            cerr << endl;
        }
    } Pivot;
    typedef struct {
        string mode;    // "query" | "pivot"
        Query query;
        Pivot pivot;
        unsigned int deadlineTimer;
        void dump(void) {
            if (mode == "query") query.dump();
            else if (mode == "pivot") pivot.dump();
            cerr << "deadlineTimer: " << deadlineTimer << endl;
        }
    } Request;
    
    static pthread_t pt;
    static bool isTimeOut;
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
        } else {
            cerr << "setTimer: " << timerInSec << " sec" << endl;
        }
        isTimeOut = false;
        pthread_create(&pt, NULL, &timerHandle, &timerInSec);
    }
    static void cancelTimer(void) {
        pthread_cancel(pt);
        if (! isTimeOut) cerr << "timer canceled" << endl;
    }
    
    static Config* mt_config;
    static Occ* mt_occ;
    Result Enum(Query& query, Ewah& dimBmp);
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
        bool evalRequestJson(Request& request);
        bool evalRequestFlat(Request& request);
        bool evalRequest(Request& request);
        
    private:
        void setQueryDefault(Query& query);
        void co_occurrence(Query& query, map<string, Result>& res);
        void axisValsList(vector<pair<char, string>>& flds, vector<vector<string>>& valsList);
        void item2traBmp(string itemKey, string itemVal, Ewah& traBmp);
        void pivot(Pivot& pivot, map<string, Result>& res);
        void proc(void) override;
    };
}

#endif /* kggolap_h */
