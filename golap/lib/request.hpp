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

#ifndef request_hpp
#define request_hpp

#include <map>
#include "cmn.hpp"
#include "config.hpp"
#include "occ.hpp"

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
    typedef struct {string key; map<string, Ewah> DimBmpList;} Dimension;
    
    /////////////////////
    struct Query {
        Ewah traFilter;
        Ewah itemFilter;
        sel_cond selCond;
        sort_key sortKey;
        size_t sendMax;
        pair<string, string> granularity;   // first:transaction granurality, second:node granurality
        Dimension dimension;
        size_t debug_mode;
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
    };
    
    /////////////////////
    struct Pivot {
        Ewah traFilter;
//        Ewah itemFilter;
        typedef pair<char, string> pivAtt_t;
        typedef vector<pivAtt_t> axis_t;        // first:[I|T] second:attName
        vector<axis_t> axes;                    // first:x-axis second:y-axis
        float cutoff;
        void dump (void) {
            cerr << "x-axis: ";
            for (auto i : axes[0]) {
                cerr << i.first << ":" << i.second << " ";
            }
            cerr << endl;
            cerr << "y-axis: ";
            for (auto i : axes[1]) {
                cerr << i.first << ":" << i.second << " ";
            }
            cerr << endl;
        }
    };
    
    /////////////////////
    struct EtcReq {
        string func;
        vector<string> args;
    };
    
    /////////////////////
    class Request {
        Occ* _occ;
        Config* _config;
        Filter* _filter;
        
    public:
        string mode;    // ”control” | "retrieve" | "query" | "pivot"
        Query query;
        Pivot pivot;
        EtcReq etcRec;  // for control or retrieve
        unsigned int deadlineTimer;
        
    public:
        Request(Config* config, Occ* occ, Filter* filter) : _config(config), _occ(occ), _filter(filter) {}
        
    private:
        Dimension makeDimBitmap(string& cmdline);
        void setQueryDefault(void);
        void evalRequestJson(string& req_msg);
        void evalRequestFlat(string& req_msg);
        
    public:
        void evalRequest(string& req_msg);
        void dump(void) {
            if (mode == "query") query.dump();
            else if (mode == "pivot") pivot.dump();
            cerr << "deadlineTimer: " << deadlineTimer << endl;
        }
    };
}

#endif /* request_hpp */
