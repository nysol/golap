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
    struct selCond {
        double minSup;
        double minConf;
        double minLift;
        double minJac;
        double minPMI;
        void dump(void) {cerr << "selCond: " << minSup << " " << minConf << " " << minLift
            << " " << minJac << " " << minPMI << endl;};
    };
    
    enum sort_key {SORT_NONE, SORT_SUP, SORT_CONF, SORT_LIFT, SORT_JAC, SORT_PMI};
//    static bool isTimeOut;

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
        
        typedef struct {
            string key;
            map<string, Ewah> SliceBmpList;
        } Slice;
        Slice makeSliceBitmap(string& cmdline);
        typedef btree::btree_multimap<float, string> Result;
        size_t sizeOfResult(Result res) {
            size_t out = 0;
            for (auto i = res.begin(), ie = res.end(); i != ie; i++) {
                out += i->second.size();
            }
            return out;
        }
//        Result Enum(struct selCond& selCond, sort_key sortKey, Ewah& TraBmp, Ewah& ItemBmp, Ewah& sliceBmp);
        void Output(Result& res);
        int run(void);
    };
    
    //
    //
    class exec : public Http {
        kgGolap* golap_;
        u_short port;
        bool closing_;
        
    public:
        exec(kgGolap* golap, asio::io_service& io_service, u_short port)
        : golap_(golap), Http(io_service, port), port(port), closing_(false) {};
//        timerInSec_(10), isTimeOut_(false) {};
        
        bool isClosing(void) {return closing_;}
        
    private:
        void proc(void) override;
    };
    
    
    static pthread_t pt;
    static bool isTimeOut;
    static void* timerHandle(void* timer) {
        static unsigned int tt = *(unsigned int*)timer;
        if (tt != 0) {
            cerr << "setTimer: " << tt << " sec" << endl;
            sleep(tt);
            isTimeOut = true;
            cerr << "time out" << endl;
        }
        return (void*)NULL;
    }
    static void setTimer(unsigned int& timerInSec) {
        isTimeOut = false;
        pthread_create(&pt, NULL, &timerHandle, &timerInSec);
    }
    static void cancelTimer(void) {
        pthread_cancel(pt);
        if (! isTimeOut) cerr << "timer canceled" << endl;
    }
    
    static Config* mt_config;
    static Occ* mt_occ;
    kgGolap::Result Enum(struct selCond& selCond, sort_key sortKey, Ewah& TraBmp, Ewah& ItemBmp,
                         string& uniqAtt, Ewah& sliceBmp);
    typedef MtQueue<pair<string, Ewah*>> mq_t;
    void MT_Enum(mq_t* mq, struct selCond selCond, sort_key sortKey, Ewah TraBmp, Ewah ItemBmp,
                 string uniqAtt, map<string, kgGolap::Result>* res);
}

#endif /* kggolap_h */
