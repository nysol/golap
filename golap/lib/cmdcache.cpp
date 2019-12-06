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

#include <stdio.h>
#include "kgCSV.h"
#include "btree.hpp"
#include "bidx-ewah.hpp"
#include "lrucache.hpp"
#include "filter.hpp"
// cmdCacheのクラス定義はfilter.hppにある

using namespace std;
using namespace kgmod;

kgmod::cmdCache::cmdCache(Config* config, kgEnv* env, const bool init)
: lru_cache(config->cmdCache_size), config(config), env(env), modCount(0) {
    fileName = config->dbDir + "/cmdCache.dat";
    if (init) {
        initFile();
    } else {
        load();
    }
};

string kgmod::cmdCache::makeKey(const string& func, const vector<string>& arg, const tra_item traitem) {
    stringstream ss;
    ss << traitem;
    string tmp;
    for (auto i = arg.begin(); i != arg.end(); i++) {
        tmp += *i;
    }
    return ss.str() + func + tmp;
}

void kgmod::cmdCache::put(const string& key, const Ewah& bmp, const bool reverse) {
    if (! config->cmdCache_enable) return;
    if (reverse) {
        cmd_cache_t::put_r(key, bmp);
    } else {
        cmd_cache_t::put(key, bmp);
    }
    checkPoint();
}

void kgmod::cmdCache::put(const string& func, const vector<string>& arg, const tra_item traitem,
                          const Ewah& bmp, const bool reverse) {
    if (! config->cmdCache_enable) return;
    string key = makeKey(func, arg, traitem);
    put(key, bmp, reverse);
}

bool kgmod::cmdCache::get(const string& key, Ewah& bmp) {
    if (! config->cmdCache_enable) return false;
    if (! cmd_cache_t::exists(key)) return false;
    bmp = cmd_cache_t::get(key);
    return true;
}

bool kgmod::cmdCache::get(const string& func, const vector<string>& arg, const tra_item traitem, Ewah& bmp) {
	cerr << "cchach get" << endl; 
    if (! config->cmdCache_enable) return false;
    string key = makeKey(func, arg, traitem);
    return get(key, bmp);
}

bool kgmod::cmdCache::exists(const string& key) {
    if (! config->cmdCache_enable) return false;
    return cmd_cache_t::exists(key);
}

bool kgmod::cmdCache::exists(const string& func, const vector<string>& arg, const tra_item traitem) {
    if (! config->cmdCache_enable) return false;
    string key = makeKey(func, arg, traitem);
    return exists(key);
}

void kgmod::cmdCache::initFile(void) {
    FILE* fp = fopen(fileName.c_str(), "w");
    if (fp == NULL) {
        stringstream msg;
        msg << "failed to open " + fileName;
        throw kgError(msg.str());
    }
    fclose(fp);
}

void kgmod::cmdCache::save(void) {
    if (! config->cmdCache_enable) return;
    if (modCount == 0) return;
    
    FILE* fp = fopen(fileName.c_str(), "wb");
    if (fp == NULL) {
        stringstream msg;
        msg << "failed to open " + fileName;
        throw kgError(msg.str());
    }
    
    try {
        for (auto i = _cache_items_list.cbegin(); i != _cache_items_list.cend(); i++) {
            size_t rc;
            char* cmd = (char*)i->first.c_str();
            size_t size = i->first.size();
            rc = fwrite(&size, sizeof(size_t), 1, fp);
            if (rc == 0) throw 0;
            rc = fwrite(cmd, size, 1, fp);
            if (rc == 0) throw 0;
            
            stringstream ss;
            i->second.write(ss);
            size_t sssize = ss.str().size();
            rc = fwrite(&sssize, sizeof(size_t), 1, fp);
            if (rc == 0) throw 0;
            rc = fwrite(ss.str().c_str(), sssize, 1, fp);
            if (rc == 0) throw 0;
        }
    } catch(int e) {
        fclose(fp);
        stringstream msg;
        msg << "failed to write " << fileName;
        throw kgError(msg.str());
    }
    
    fclose(fp);
}

void kgmod::cmdCache::load(void) {
    if (! config->cmdCache_enable) return;
    
    FILE* fp = fopen(fileName.c_str(), "rb");
    if (fp == NULL) {
        stringstream msg;
        msg << "failed to open " + fileName;
        throw kgError(msg.str());
    }
    
    try {
        while (true) {
            size_t rc;
            size_t keyLen;
            rc = fread(&keyLen, sizeof(keyLen), 1, fp);
            if (rc == 0) break;
            char* keyBuf = (char*)malloc(keyLen + 1);
            rc = fread(keyBuf, keyLen, 1, fp);
            keyBuf[keyLen] = '\0';
            if (rc == 0) {free(keyBuf); throw 1;}
            string key(keyBuf);
            
            size_t ssSize;
            rc = fread(&ssSize, sizeof(ssSize), 1, fp);
            if (rc == 0) {free(keyBuf); throw 1;}
            char* ssBuf = (char*)malloc(ssSize);
            rc = fread(ssBuf, ssSize, 1, fp);
            if (rc == 0) {free(keyBuf); free(ssBuf); throw 1;}
            
            stringstream ss;
            ss.write(ssBuf, ssSize);
            Ewah bmp;
            bmp.read(ss);
            put(key, bmp, true);
            
            free(keyBuf); free(ssBuf);
        }
    } catch(int e) {
        fclose(fp);
        stringstream msg;
        msg << "failed to read " << fileName;
        throw kgError(msg.str());
    }
    
    fclose(fp);
}

void kgmod::cmdCache::clear(void) {
    _cache_items_list.clear();
    _cache_items_map.clear();
}

void kgmod::cmdCache::dump(bool debug) {
    if (! config->cmdCache_enable) return;
    if (! debug) return;
    
    cerr << "<<< dump command cache >>>" << endl;
    for (auto i = _cache_items_list.cbegin(); i != _cache_items_list.cend(); i++) {
        cerr << i->first << "-> ";
        Ewah bmp = i->second;
//        bmp.printout(cerr);
        Cmn::CheckEwah(bmp);
    }
    cerr << endl;
}

void kgmod::cmdCache::checkPoint(void) {
    modCount++;
    if (modCount > config->cmdCache_saveInterval) {
        save();
        modCount = 0;
    }
}
