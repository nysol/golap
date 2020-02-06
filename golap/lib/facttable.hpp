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

#ifndef facttable_hpp
#define facttable_hpp

#include <unordered_map>
#include <boost/functional/hash.hpp>
#include <boost/optional.hpp>
#include <kgConfig.h>
#include <kgmod.h>
#include "bidx-ewah.hpp"
#include "config.hpp"
#include "traatt.hpp"
#include "itematt.hpp"
#include "occ.hpp"
#include "btree.hpp"

using namespace std;

namespace kgmod {
    typedef double factVal_t;
    
    ////
    class AggrFunc {
        string _original;
        struct elem {
            char ope;
            string func;
            factVal_t val;
            int fldPos;
        };
        vector<elem> _stack;
        string _func;
        unordered_map<string, int>* _valPosMap;     // ["value name"] -> position in csv columns
        const map<string, int> _funcs = {{"COUNT",0}, {"SUM",1}, {"AVE",1}, {"MED",1}, {"MIN",1},
                                         {"MAX",1}, {"VAR",1}, {"SD",1}, {"QTILE",1}};
                                                    // ["function name"] -> argument count
        
    private:
        string getOneElem(char** ptr);
        void eval(void);
        
    public:
        void setValPosMap(unordered_map<string, int>* valPosMap) {_valPosMap = valPosMap;}
        string& getOriginal(void) {return _original;};
        string& getFunc(void) {return _func;};
        void parse(const string& cmd);
        factVal_t calc(vector<vector<factVal_t>>& dat);
        void dump(void);
    };
    
    ////
    class FactTable {
        Config* _config;
        kgEnv* _env;
        Occ* _occ;
        
        string _key2recFile;
        
        typedef vector<factVal_t> vals_t;
        typedef map<size_t, vals_t> itemMap_t;
        
        // numField only
        vector<itemMap_t> _numFldTbl;           // [traNo][itemNo] -> binary value (numField only)
        unordered_map<string, int> _numFldPos;  // [fieldName] -> _numFldPos (numField only)
        
        // both of numField and strField
        typedef pair<size_t, size_t> addr_t;
        vector<addr_t> _addr;                   // [factTable recNo] -> {traNo, itemNo}
//        btree::btree_multimap<addr_t, size_t> _recNo; // [{traNo, itemNo}] -> factTable recNo
        multimap<addr_t, size_t> _recNo; // [{traNo, itemNo}] -> factTable recNo
        
    public:
        size_t recMax;
        BTree bmplist;                          // [{key, value}] -> bitmap of facttable recNo

    public:
        FactTable(Config* config, kgEnv* env, Occ* occ);
        
    private:
        boost::optional<int> fldPos(const string fld);
        void item2traBmp(const Ewah& itemBmp, Ewah& traBmp);
        bool getVals(const size_t trano, const size_t itmno, vals_t*& vals);
        bool getItems(const size_t traNo, Ewah& itemBmp);
        
    public:
        void build(void);
        void save(const bool clean);
        void load(void);
        void recNo2keys(size_t recNo, size_t& traNo, size_t& itemNo) {
            addr_t& tmp = _addr[recNo];
            traNo = tmp.first;
            itemNo = tmp.second;
        }
        bool existInFact(const size_t traNo, const size_t itemNo, const Ewah& factBmp) {
            bool stat = false;
            for (auto r = _recNo.lower_bound({traNo, itemNo}), er = _recNo.end(); r != er; r++) {
                if (r->first.first != traNo || r->first.second !=itemNo) break;
                size_t rn = r->second;
                stat = factBmp.get(rn);
                if (stat) break;
            }
            return stat;
        }
        bool existInFact(const Ewah& traNo, const size_t itemNo, const Ewah& factBmp) {
            bool stat = false;
            for (auto i = traNo.begin(), ei = traNo.end(); i != ei; i++) {
                stat = existInFact(*i, itemNo, factBmp);
                if (stat) break;
            }
            return stat;
        }
        
        void toTraItemBmp(const Ewah& factFilter, const Ewah& itemFilter, Ewah& traBmp, Ewah& itemBmp);
//        void toItemBmp(const Ewah& factFilter, const Ewah& traFilter,
//                       const Ewah& itemFilter, Ewah& itemBmp);
        size_t valCount(void) {return _numFldPos.size();}
        unordered_map<string, int>* valPosMap(void) {return &_numFldPos;};
        string valNames(void);
        void funcParse(const string& func, vector<string>& f);
        void setFlds(const vector<string>& flds) {
            for (int i = 0; i < flds.size(); i++) _numFldPos[flds[i]] = i;
        }
        size_t aggregate(const pair<string&, Ewah&>& traBmp, const pair<string&, Ewah&>& itemBmp,
                         vector<pair<AggrFunc, string>>& vals, string& line);
        void dump(void);
    };
}

#endif /* facttable_hpp */
