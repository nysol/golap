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
        typedef vector<factVal_t> vals_t;
        typedef pair<size_t, size_t> addr_t;
        typedef map<size_t, vals_t> itemMap_t;
        vector<itemMap_t> _factTable;       // [traNo][itemNo]
        vector<string> _flds;
        unordered_map<string, int> _fldPos;
        // どこで実装すべきか？？？
        typedef btree::btree_multimap<string, size_t> valsIndex_t;
        vector<valsIndex_t> _valsIndex;     // [_fldPos][value] -> index in _factTable
        
    public:
        FactTable(Config* config, kgEnv* env, Occ* occ)
        : _config(config), _env(env), _occ(occ) {};
        
    private:
        boost::optional<int> fldPos(const string fld);
        void item2traBmp(const Ewah& itemBmp, Ewah& traBmp);
        bool getVals(const size_t trano, const size_t itmno, vals_t*& vals);
        bool getItems(const size_t traNo, Ewah& itemBmp);
        
    public:
        void load(void);
        size_t valCount(void) {return _fldPos.size();}
        unordered_map<string, int>* valPosMap(void) {return &_fldPos;};
        string valNames(void);
        void funcParse(const string& func, vector<string>& f);
        void setFlds(const vector<string>& flds) {
            for (int i = 0; i < flds.size(); i++) _fldPos[flds[i]] = i;
        }
        size_t aggregate(const pair<string&, Ewah&>& traBmp, const pair<string&, Ewah&>& itemBmp,
                         vector<pair<AggrFunc, string>>& vals, string& line);
    };
}

#endif /* facttable_hpp */
