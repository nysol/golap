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
#include <stdlib.h>
#include <string>
#include <vector>
#include "kgCSV.h"
#include <kgError.h>
#include "btree.hpp"
#include "bidx-ewah.hpp"
#include "cmn.hpp"
#include "occ.hpp"
#include "traatt.hpp"

using namespace std;
using namespace kgmod;
using namespace kglib;

kgmod::Occ::Occ(Config* config, kgEnv* env) : _config(config), _env(env) {

    _dbName = config->dbDir + "/coitem.dat";
    _liveTraFile = config->dbDir + "/livetra.dat";
    
    string dtmDb = config->dbDir + "/occ.dtm";
    string occDb = config->dbDir + "/occ.dat";
    bmpList.PutDbName(dtmDb, occDb);
    
    //string dtmDb2 = config->dbDir + "/occ2.dtm";
    //string occDb2 = config->dbDir + "/occ2.dat";
    //exBmpList.PutDbName(dtmDb2, occDb2);
    

    traAtt = new class TraAtt(config, env);
    itemAtt = new class ItemAtt(config, env);
    occKey = config->traFile.itemFld;
    bmpList.InitKey(occKey, config->itemDataType[occKey]);
}

kgmod::Occ::~Occ(void) {
    delete traAtt;
    delete itemAtt;
}


void kgmod::Occ::build(void) {

	traAtt->build(bmpList);

	itemAtt->build();

}

void kgmod::Occ::saveLiveTra(void) {
    cerr << "number of transactions: " << LiveTraCnt() << endl;
    FILE* fp = fopen(_liveTraFile.c_str(), "wb");
    if (fp == NULL) {
        stringstream msg;
        msg << "failed to open " + _liveTraFile;
        throw kgError(msg.str());
    }
    try {
        size_t rc;
        stringstream ss;
        liveTra.write(ss);
        size_t size = ss.str().length();
        rc = fwrite(&size, sizeof(size_t), 1, fp);
        if (rc == 0) throw 0;
        rc = fwrite(ss.str().c_str(), size, 1, fp);
        if (rc == 0) throw 0;
    } catch(int e) {
        fclose(fp);
        stringstream msg;
        msg << "failed to write " << _liveTraFile;
        throw kgError(msg.str());
    }
    fclose(fp);
}

void kgmod::Occ::saveCooccur(const bool clean) {

    if (clean) {
        FILE* fp = fopen(_dbName.c_str(), "wb");
        fclose(fp);
    }

    cerr << "writing " << _dbName << " ..." << endl;
    FILE* fp = fopen(_dbName.c_str(), "ab");
    if (fp == NULL) {
        stringstream msg;
        msg << "failed to open " + _dbName;
        throw kgError(msg.str());
    }
    try {
        for (size_t i = 0; i < occ.size(); i++) {
            size_t rc;
            stringstream ss;
            occ[i].write(ss);
            size_t size = ss.str().length();
            rc = fwrite(&size, sizeof(size_t), 1, fp);
            if (rc == 0) throw 0;
            rc = fwrite(ss.str().c_str(), size, 1, fp);
            if (rc == 0) throw 0;
        }
    } catch(int e) {
        fclose(fp);
        stringstream msg;
        msg << "failed to write " << _dbName;
        throw kgError(msg.str());
    }
    fclose(fp);
}

void kgmod::Occ::save(const bool clean) {
    traAtt->save(clean);
    itemAtt->save(clean);
    bmpList.save(clean);
    //exBmpList.save(clean);
    saveCooccur(clean);
    saveLiveTra();
}

void kgmod::Occ::loadActTra(void) {
    cerr << "reading actual transactions bitmap" << endl;
    FILE* fp = fopen(_liveTraFile.c_str(), "rb");
    if (fp == NULL) {
        stringstream msg;
        msg << "failed to open " + _liveTraFile;
        throw kgError(msg.str());
    }
    try {
        size_t size = 0;
        size_t rc = 0;
        rc = fread(&size, sizeof(size_t), 1, fp);
        if (rc == 0) throw 0;
        char* buf = (char *)malloc(size);
        rc = fread(buf, size, 1, fp);
        if (rc == 0) {free(buf); throw 0;}
        
        stringstream ss;
        ss.write(buf, size);
        liveTra.read(ss);
        
        free(buf);
    } catch(int e) {
        fclose(fp);
        stringstream msg;
        msg << "failed to read " << _liveTraFile;
        throw kgError(msg.str());
    }
    fclose(fp);
    cerr << "number of transactions: " << LiveTraCnt() << endl;
}

void kgmod::Occ::loadCooccur(void) {
    cerr << "loading co-occurrence" << endl;
    occ.resize(traAtt->traMax + 1);
    
    FILE* fp = fopen(_dbName.c_str(), "rb");
    if (fp == NULL) {
        stringstream msg;
        msg << "failed to open " + _dbName;
        throw kgError(msg.str());
    }
    try {
        size_t i = 0;
        while (true) {
            size_t size = 0;
            size_t rc = 0;
            rc = fread(&size, sizeof(size_t), 1, fp);
            if (rc == 0) break;
            char* buf = (char *)malloc(size);
            rc = fread(buf, size, 1, fp);
            if (rc == 0) {free(buf); throw 1;}
            
            stringstream ss;
            ss.write(buf, size);
            occ[i].read(ss);
            
            free(buf);
            i++;
        }
    } catch(int e) {
        fclose(fp);
        stringstream msg;
        msg << "failed to read " << _dbName;
        throw kgError(msg.str());
    }
    fclose(fp);
}

void kgmod::Occ::load(void) {
    traAtt->load();
    itemAtt->load();
    bmpList.load();
    //exBmpList.load();
    loadCooccur();
    itemAtt->buildKey2attMap();
    loadActTra();
}
/*つかってない
void kgmod::Occ::item2traBmp(string& itemKey, string& itemVal, Ewah& traBmp) {
    if (exBmpList.GetVal(itemKey, itemVal, traBmp)) return;
    
    cerr << "calc item to transaction bitmap: " << itemKey << ":" << itemVal << endl;
    exBmpList.InitKey(itemKey, STR);
    traBmp.reset();
    Ewah itemBmp;
    itemAtt->bmpList.GetVal(itemKey, itemVal, itemBmp);
    for (auto i = itemBmp.begin(), ie = itemBmp.end(); i != ie; i++) {
        string tar = itemAtt->item[*i];
        Ewah tmp = bmpList.GetVal(occKey, tar);
        traBmp = traBmp | tmp;
    }
    exBmpList.SetVal(itemKey, itemVal, traBmp);
}*/

// 指定したトランザクション(traNo)から指定したトランザクション属性(traAttKey)と同じ値を持つ
// トランザクションを抽出し、それらのトランザクションに含まれるitemNoのビットマップを生成して。
// ex_occにキャッシュさせる。
//　更に、traFilterに含まれるitemだけを抽出して返す。（ex_occ_CacheOnceQueryにキャッシュ）
/*
void kgmod::Occ::expandItemByGranu(const size_t traNo, const vector<string>& traAttKey,
                                   const Ewah& traFilter, const Ewah& itemFilter, Ewah& itemBmp,
                                   unordered_map<string, Ewah>& ex_occ_CacheOnceQeuery) {
    string joinedTraAttKey = Cmn::CsvStr::Join(traAttKey, ":");
    vector<string> traAttVal;
    traAtt->traNo2traAtt(traNo, traAttKey, traAttVal);
    string joinedTraAttVal = Cmn::CsvStr::Join(traAttVal, ":");
    
    // キャッシュにあれば、それを返す
    auto it = ex_occ_CacheOnceQeuery.find(joinedTraAttVal);
    if (it != ex_occ_CacheOnceQeuery.end()) {
        itemBmp = it->second;
        return;
    }
    
    Ewah granuBmp;      // これはtransaction bitmap
    granuBmp.padWithZeroes(itemAtt->itemMax + 1);
    granuBmp.inplace_logicalnot();
    bmpList.GetVal(traAttKey, traAttVal, granuBmp);
    
    Ewah cachedItemBmp;
    bool cached = ex_occ.GetVal(joinedTraAttKey, joinedTraAttVal, cachedItemBmp);
    if (!cached) {
        // ex_occになければ、当該item bitmapを生成する
//        cerr << "make transaction attribute bitmap (exocc) derived from traNo=";
//        cerr << traNo << " ";
//        cerr << joinedTraAttKey << "=" << joinedTraAttVal << endl;
        cachedItemBmp.reset();
        for (auto t = granuBmp.begin(), et = granuBmp.end(); t != et; t++) {
            cachedItemBmp = cachedItemBmp | occ[*t];
        }
        // ex_occにキャッシュ
        ex_occ_mtx.lock();
        ex_occ.InitKey(joinedTraAttKey, STR);
        ex_occ.SetVal(joinedTraAttKey, joinedTraAttVal, cachedItemBmp);
        ex_occ_mtx.unlock();
    }
    
    // キャッシュされたitem bmp(cachedItemBmp)よりtraFilterのトランザクションに含まれない
    // itemをommitしながらitem bmpを作成する（結果をキャッシュする）
    Ewah zeroBmp;
    itemBmp.reset();
    Ewah tmpTraFilter = traFilter & granuBmp;
    Ewah tmpItemBmp = cachedItemBmp & itemFilter;
    for (auto i = tmpItemBmp.begin(), ei = tmpItemBmp.end(); i != ei; i++) {
        string tarItem = itemAtt->item[*i];
        Ewah tmpTraBmp = bmpList.GetVal(occKey, tarItem);
        Ewah tmpTraBmp2 = tmpTraBmp & tmpTraFilter;
        if (tmpTraBmp2 != zeroBmp) {
            itemBmp.set(*i);
        }
    }
    ex_occ_CacheOnceQeuery[joinedTraAttVal] = itemBmp;
}
*/
void kgmod::Occ::expandItemByGranu(const size_t traNo, const vector<string>& traAttKey, const Ewah& traFilter,
                                   const Ewah& itemFilter, map<size_t, Ewah>& ex_occ,
                                   map<string, map<size_t, Ewah>>& ex_occ_cacheOnceQuery) {
    
    string joinedTraAttKey = Cmn::CsvStr::Join(traAttKey, ":");
    vector<string> traAttVal;
    traAtt->traNo2traAtt(traNo, traAttKey, traAttVal);
    string joinedTraAttVal = Cmn::CsvStr::Join(traAttVal, ":");
    auto it = ex_occ_cacheOnceQuery.find(joinedTraAttVal);
    if (it != ex_occ_cacheOnceQuery.end()) {
        ex_occ = it->second;
        return;
    }
    
    Ewah traBmp;
    traBmp.padWithZeroes(traAtt->traMax + 1);
    traBmp.inplace_logicalnot();
    bmpList.GetVal(traAttKey, traAttVal, traBmp);
    traBmp = traBmp & traFilter;
    
    for (auto t = traBmp.begin(), et = traBmp.end(); t != et; t++) {
        Ewah tmp = occ[*t] & itemFilter;
        ex_occ.insert({*t, tmp});
    }
    
    ex_occ_cacheOnceQuery.insert({joinedTraAttVal, ex_occ});
}




/*
size_t kgmod::Occ::itemFreq(const size_t itemNo, const Ewah& traFilter, const vector<string>* tra2key) {
    size_t cnt = 0;
    unordered_map<string, int> checkedAttVal;
    Ewah traBmp = bmpList[{_config->traFile.itemFld, itemAtt->item[itemNo]}];
    traBmp = traBmp & traFilter;
    
    if (tra2key == NULL) {
        cnt = traBmp.numberOfOnes();
    } else {
        for (auto t = traBmp.begin(), et = traBmp.end(); t != et; t++) {
            string val = (*tra2key)[*t];
            if (checkedAttVal.find(val) == checkedAttVal.end()) {
                cnt++;
                checkedAttVal[val] = 1;
            }
        }
    }
    return cnt;
}

size_t kgmod::Occ::itemFreq(size_t itemNo, vector<string>* tra2key) {
    return bmpList[{_config->traFile.itemFld, itemAtt->item[itemNo]}].numberOfOnes();
}
*/

size_t kgmod::Occ::attFreq(const vector<string>& attKeys, const vector<string>attVals,
                           const Ewah& traFilter, const Ewah& itemFilter, const vector<string>* tra2key) {
    size_t cnt = 0;
    Ewah itemBmp;
    itemBmp.padWithZeroes(itemAtt->itemMax + 1); itemBmp.inplace_logicalnot();
    for (size_t i = 0; i < attKeys.size(); i++) {
        Ewah *tmpItemBmp;
        if (! itemAtt->bmpList.GetVal(attKeys[i], attVals[i], tmpItemBmp)) continue;
        itemBmp = itemBmp & *tmpItemBmp;
    }
    itemBmp = itemBmp & itemFilter;
    
    Ewah traBmp;
    for (auto it = itemBmp.begin(), eit = itemBmp.end(); it != eit; it++) {
        Ewah* tmpTraBmp;
        if (! bmpList.GetVal(_config->traFile.itemFld, itemAtt->item[*it], tmpTraBmp)) continue;
        traBmp = traBmp | *tmpTraBmp;
    }
    // traBmp = traBmp & traFilter;なせいらなくなった？
    
    if (tra2key == NULL) {
        cnt = traBmp.numberOfOnes();
    } else {

        set<string> checkedAttVal;
        for (auto t = traBmp.begin(), et = traBmp.end(); t != et; t++) {
            string val = (*tra2key)[*t];
            if (checkedAttVal.find(val) == checkedAttVal.end()) {
                cnt++;
                checkedAttVal.insert(val);
            }
        }
    }
    return cnt;
}

size_t kgmod::Occ::attFreq(string& attKey, string& attVal, const Ewah& traFilter,
                           const Ewah& itemFilter, const vector<string>* tra2key) {

    Ewah *itemBmp;
    if (!itemAtt->bmpList.GetVal(attKey, attVal, itemBmp)) return 0;
    Ewah itemVals = *itemBmp & itemFilter;

    Ewah traBmp;
    for (auto i = itemVals.begin(); i != itemVals.end(); i++) {
        Ewah tmp;
        if (! bmpList.GetVal(_config->traFile.itemFld, itemAtt->item[*i], tmp)) continue;
        traBmp = traBmp | tmp;
    }

    traBmp = traBmp & traFilter;

    size_t cnt = 0;
    unordered_map<string, int> checkedAttVal;
    if (tra2key == NULL) {
        cnt = traBmp.numberOfOnes();
    } else {
        for (auto t = traBmp.begin(), et = traBmp.end(); t != et; t++) {
            string val = (*tra2key)[*t];
            if (checkedAttVal.find(val) == checkedAttVal.end()) {
                cnt++;
                checkedAttVal[val] = 1;
            }
        }
    }
    return cnt;
}
/*
size_t kgmod::Occ::attFreq(string attKey, string attVal, const Ewah& itemFilter,
                           const vector<string>* tra2key) {
    if (attKey == "0011023580813") {
        cerr << endl;
    }
    size_t out = 0;
    Ewah *itemBmp;
    itemAtt->bmpList.GetVal(attKey, attVal, itemBmp);
    Ewah itemVals = *itemBmp & itemFilter;
    for (auto i = itemVals.begin(); i != itemVals.end(); i++) {
        size_t c = bmpList[{_config->traFile.itemFld, itemAtt->item[*i]}].numberOfOnes();
        //    itemFreq(*i, tra2key);
        out += c;
    }
    return out;
}
*/

void kgmod::Occ::dump(const bool debug) {
    if (! debug) return;
    
    traAtt->dump(debug);
    itemAtt->dump(debug);
    
    cerr << "<<< dump occ >>>" << endl;
    bmpList.dump(debug);
    cerr << endl;

    cerr << "<<< dump key2attmap >>>" << endl;
    itemAtt->dumpKey2attMap(debug);
    cerr << endl;

/* ===================
    for (size_t i = 0; i < occ.size(); i++) {
        cerr << "occ[" << i << "] ";
        Cmn::CheckEwah(occ[i]);
    }
===================== */
}

size_t kgmod::Occ::countKeyValue(const vector<string>& keys, Ewah& traFilter) {
    vector<size_t> traAttKeyPos(keys.size());
    for (size_t i = 0; i < keys.size(); i++) {
        boost::optional<size_t> pos = Cmn::posInVector(_config->traAttFile.granuFields, keys[i]);
        if (! pos) {
            string msg = keys[i] + " is not set in config file (traAttFile.granuFields)\n";
            throw kgError(msg);
        }
        traAttKeyPos[i] = *pos;
    }
    
    size_t cnt = 0;
    unordered_map<string, bool> checked_vals;
    for (auto tra = traFilter.begin(), etra = traFilter.end(); tra != etra; tra++) {
        string valList;
        for (const auto pos : traAttKeyPos) {
            string val(traAtt->traAttMap[*tra][pos]);
            valList += val;
        }
        if (checked_vals.find(valList) == checked_vals.end()) {
            checked_vals[valList] = true;
            cnt++;
        }
    }
    return cnt;
}

void kgmod::Occ::getTra2KeyValue(string& key, vector<string>& tra2key) {
    tra2key.resize(traAtt->traMax + 1);
    vector<string> vals = evalKeyValue(key);
    for (auto& val : vals) {
        Ewah* tras;
        bmpList.GetVal(key, val, tras);
        for (auto i = tras->begin(), ei = tras->end(); i != ei; i++) {
            tra2key[*i] = val;
        }
    }
}

void kgmod::Occ::getTra2KeyValue(const vector<string>& keys, vector<string>& tra2key) {
    vector<size_t> traAttKeyPos(keys.size());
    for (size_t i = 0; i < keys.size(); i++) {
        boost::optional<size_t> pos = Cmn::posInVector(_config->traAttFile.granuFields, keys[i]);
        if (! pos) {
            string msg = keys[i] + " is not set in config file (traAttFile.granuFields)\n";
            throw kgError(msg);
        }
        traAttKeyPos[i] = *pos;
    }

    tra2key.resize(traAtt->traMax + 1);
    for (size_t tra = 0; tra < tra2key.size(); tra++) {
        for (const auto pos : traAttKeyPos) {
            string val(traAtt->traAttMap[tra][pos]);
            tra2key[tra] += val + ":";
        }
        Cmn::EraseLastChar(tra2key[tra]);
    }
}

void kgmod::Occ::filterItemBmpByTraBmp(const Ewah& itemBmp, const Ewah& traBmp, Ewah& filteredItemBmp) {
    filteredItemBmp.reset();
    for (auto i = itemBmp.begin(), ei = itemBmp.end(); i != ei; i++) {
        Ewah* tmpTraBmp;
        if (! bmpList.GetVal(occKey, itemAtt->item[*i], tmpTraBmp)) continue;
        Ewah tmp = *tmpTraBmp & traBmp;
        if (tmp.numberOfOnes() != 0) {
            filteredItemBmp.set(*i);
        }
    }
}
