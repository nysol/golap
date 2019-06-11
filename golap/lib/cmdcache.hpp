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

#ifndef cmdcache_hpp
#define cmdcache_hpp

/*****************************************************
#include <kgConfig.h>
#include <kgmod.h>
#include "bidx-ewah.hpp"
#include "lrucache.hpp"
#include "filter.hpp"

namespace kgmod {
    typedef cache::lru_cache<pair<tra_item, string>, Ewah> cmd_cache_t;
    class cmdCache : public cmd_cache_t {
        Config* config;
        kgEnv* env;
        string fileName;
        
    public:
        cmdCache(Config* config, kgEnv* env);
        string makeKey(string& func, vector<string>& arg);
        void put(tra_item traitem, string& key, Ewah& bmp);
        bool get(string& key, Ewah& bmp);
        bool exists(string& key);
        void touchFile(void);
        void save(void);
        void load(void);
        void clear(void);
        void dump(bool debug);
    };
}
****************************************/

#endif /* cmdcache_hpp */
