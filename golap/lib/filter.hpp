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

#ifndef trafilter_hpp
#define trafilter_hpp

#include <kgConfig.h>
#include <kgmod.h>
#include "btree.hpp"
#include "cmn.hpp"
#include "lrucache.hpp"
#include "occ.hpp"
#include "facttable.hpp"

namespace kgmod {
    enum tra_item : unsigned int {TRA, ITEM, SLICE , FACT};
    
    typedef typename cache::lru_cache<string, Ewah> cmd_cache_t;

    class cmdCache : public cmd_cache_t {
        Config* config;
        //kgEnv* env;
        string fileName;
        size_t modCount;
        
	    public:
        //cmdCache(Config* config, kgEnv* env, const bool init);
        cmdCache(Config* config,const bool init);
        
        string makeKey(const string& func, const vector<string>& arg, const tra_item traitem);
        void put(const string& key, const Ewah& bmp, const bool reverse=false);
        void put(const string& func, const vector<string>& arg, const tra_item traitem,
                 const Ewah& bmp, const bool reverse=false);
        bool get(const string& key, Ewah& bmp);
        bool get(const string& func, const vector<string>& arg, const tra_item traitem, Ewah& bmp);
        bool exists(const string& key);
        bool exists(const string& func, const vector<string>& arg, const tra_item traitem);
        void initFile(void);
        void save(void);
        void load(void);
        void clear(void);
        void dump(bool debug);
        
  	  private:
        void checkPoint(void);
    };
    
    //
    //
    class Filter {
        Occ* occ;
        FactTable* fact;
        cmdCache* cache;
        Config* config;
        kgEnv* env;
        bool debug = false;
        
    public:
        Filter(Occ* occ,FactTable* fact , cmdCache* cmdcache, Config* config, kgEnv* env, bool debug)
            : occ(occ), fact(fact),cache(cmdcache), config(config), env(env), debug(debug) {};
        
        Ewah makeTraBitmap(string& cmdline);
        Ewah makeItemBitmap(string& cmdline);
        Ewah makeFactBitmap(string& cmdline);
        
    private:
        bool existsFldName(const string& fldName, const tra_item traitem);
        Ewah& logicalnot(Ewah& bmp, const tra_item traitem);
        Ewah allone(const tra_item traitem);

        BTree* setModeTraItem(const tra_item traitem);
        pair<BTree*, string> setModeTraItem2(const tra_item traitem);
        
        typedef vector<string> values_t;
        Ewah sel(const values_t& values, const tra_item traitem);                 // configのtra名、item名を用いてsel
        Ewah del(const values_t& values, const tra_item traitem);                 // configのtra名、item名を用いてdel
        Ewah isin(const string& key, const values_t& values, const tra_item traitem);
        Ewah isnotin(const string& key, const values_t& values, const tra_item traitem);
        Ewah like(const string& key, const values_t& values, const tra_item traitem);
        Ewah search(const string& mothod, const string& key, const values_t& values, const tra_item traitem);
        Ewah range(const string& key, const pair<string, string>& values, const tra_item traitem);
        
        // tra専用
        Ewah sel_item(string& itemFilter, const tra_item traitem);
        Ewah del_item(string& itemFilter, const tra_item traitem);
        Ewah having(const string& key, string& andor, string& itemFilter, const tra_item traitem);
        
        void go_next(char** cmdPtr);
        string getFunc(char** cmdPtr);
        values_t getArg(char** cmdPtr);
        string getString(char** cmdPtr);
        Ewah runfunc(char** cmdPtr, const tra_item traitem);
        Ewah runcmd(char** cmdPtr, const tra_item traitem);
    };
}

#endif /* trafilter_hpp */
