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

#ifndef mbindex_h
#define mbindex_h

#include <pthread.h>
#include <map>
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

using namespace std;
using namespace kglib;

namespace kgmod {
    struct selCond {
        double minSup;
        double minConf;
        double minLift;
        double minJac;
        void dump(void) {cerr << "selCond: " << minSup << " " << minConf << " " << minLift << " " << minJac << endl;};
    };
    
    enum sort_key {SORT_NONE, SORT_SUP, SORT_CONF, SORT_LIFT, SORT_JAC};
    static bool isTimeOut;

    class kgGolap : public kgMod {
    private:
        string opt_inf;
        pthread_t pt;
        
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
        
        typedef btree::btree_multimap<float, string> Result;
        Result Enum(struct selCond& selCond, sort_key sortKey, Ewah& TraBmp, Ewah& ItemBmp);
        void Output(Result& res);
        int run(void);

        static void* timerHandle(void* timer) {
            static unsigned int tt = *(unsigned int*)timer;
            cerr << "setTimer: " << tt << " sec" << endl;
            sleep(tt);
            isTimeOut = true;
            cerr << "time out" << endl;
            return (void*)NULL;
        }
        void setTimer(unsigned int& timerInSec) {
            isTimeOut = false;
            pthread_create(&pt, NULL, &kgmod::kgGolap::timerHandle, &timerInSec);
        }
        void cancelTimer(void) {
            pthread_cancel(pt);
            if (! isTimeOut) cerr << "timer canceled" << endl;
        }
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
}

#endif /* mbindex_h */
