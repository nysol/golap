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
    
    string dtmDb2 = config->dbDir + "/occ2.dtm";
    string occDb2 = config->dbDir + "/occ2.dat";
    exBmpList.PutDbName(dtmDb2, occDb2);
    
    string ex_dtmDb = config->dbDir + "/exocc.dtm";
    string ex_occDb = config->dbDir + "/exocc.dat";
    ex_occ.PutDbName(ex_dtmDb, ex_occDb);
    
    traAtt = new class TraAtt(config, env);
    itemAtt = new class ItemAtt(config, env);
    occKey = config->traFile.itemFld;
    bmpList.InitKey(occKey, config->itemDataType[occKey]);
}

kgmod::Occ::~Occ(void) {
    delete traAtt;
    delete itemAtt;
}

void kgmod::Occ::buildExBmpList(void) {
    cerr << "building extra transaction index" << endl;
    vector<vector<string>*> AllItemAtt(2);
    AllItemAtt[0] = &(_config->itemAttFile.strFields);
    AllItemAtt[1] = &(_config->itemAttFile.catFields);
    
    for (size_t i = 0; i < 2; i++) {
        for (auto& key : *AllItemAtt[i]) {
            exBmpList.InitKey(key, STR);
            vector<string> allVals = itemAtt->bmpList.EvalKeyValue(key);
            for (auto& val : allVals) {
                Ewah itemBmp;
                itemAtt->bmpList.GetVal(key, val, itemBmp);
                Ewah traBmp;
                cerr << key << "," << val << ": ";
                for (auto i = itemBmp.begin(), ie = itemBmp.end(); i != ie; i++) {
                    string tar = itemAtt->item[*i];
                    Ewah tmp = bmpList.GetVal(occKey, tar);
                    traBmp = traBmp | tmp;
                }
                exBmpList.SetVal(key, val, traBmp);
//                Cmn::CheckEwah(exBmpList[{key, val}]);
            }
        }
    }
}

void kgmod::Occ::build(void) {
    traAtt->build(bmpList);
    itemAtt->build();
    
    cerr << "building transaction index" << endl;
    kgCSVfld tra;
    tra.open(_config->traFile.name, _env, false);
    tra.read_header();
    vector<string> fldName = tra.fldName();
    string traNameFld;
    int traNameFldPos = -1;
    string itemNameFld;
    int itemNameFldPos = -1;
    for (int i = 0; i < fldName.size(); i++) {
        if (fldName[i] == _config->traFile.traFld) {
            traNameFld = _config->traFile.traFld;
            traNameFldPos = i;
        } else if (fldName[i] == _config->traFile.itemFld) {
            itemNameFld = _config->traFile.itemFld;
            itemNameFldPos = i;
        }
    }
    
    if (traNameFldPos == -1) {
        stringstream ss;
        ss << traNameFld << " is not found on " << _config->traFile.name;
        throw kgError(ss.str());
    }
    if (itemNameFldPos == -1) {
        stringstream ss;
        ss << itemNameFld << " is not found on " << _config->traFile.name;
        throw kgError(ss.str());
    }
    
    map<string, bool> checkedTra;
    for (auto i = traAtt->traNo.begin(), ie = traAtt->traNo.end(); i != ie; i++) {
        checkedTra[i->first] = false;
    }
    
    bool isError = false;
    set<string> errKeyList;
    while (tra.read() != EOF) {
        string traName = tra.getVal(traNameFldPos);
        string itemName = tra.getVal(itemNameFldPos);
        
        bool errThisTime = false;
        if (traAtt->traNo.find(traName) == traAtt->traNo.end()) {
            isError = true;
            string buf = traNameFld + ":" + traName;
            if (errKeyList.find(buf) == errKeyList.end()) {
                stringstream ss;
                ss << "#ERROR# " << buf << " is not found on " << _config->traAttFile.name;
                cerr << ss.str() << endl;
                errKeyList.insert(buf);
            }
            errThisTime = true;
        }
        if (itemAtt->itemNo.find(itemName) == itemAtt->itemNo.end()) {
            isError = true;
            string buf = itemNameFld + ":" + itemName;
            if (errKeyList.find(buf) == errKeyList.end()) {
                stringstream ss;
                ss << "#ERROR# " << buf << " is not found on " << _config->itemAttFile.name;
                cerr << ss.str() << endl;
                errKeyList.insert(buf);
            }
            errThisTime = true;
        }
        if (errThisTime) continue;
        
        bmpList.SetBit(occKey, itemName, traAtt->traNo[traName]);
        if (traAtt->traNo[traName] >= occ.size()) occ.resize(traAtt->traNo[traName] + 1);
        occ[traAtt->traNo[traName]].set(itemAtt->itemNo[itemName]);
        checkedTra[traName] = true;
    }
    tra.close();
    
    liveTra.padWithZeroes(traAtt->traMax + 1); liveTra.inplace_logicalnot();
    for (auto i = checkedTra.begin(), ie = checkedTra.end(); i != ie; i++) {
        if (i->second) continue;
        cerr << "#WARNING# " << traNameFld << ":" << i->first << " does not exist on " << _config->traFile.name << endl;
        
        Ewah tmp; tmp.set(traAtt->traNo[i->first]);
        liveTra = liveTra - tmp;
    }
    
    if (isError) throw kgError("error occurred in building transaction index");
    
    //
//    buildExBmpList();
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
//    cerr << occ.size() << endl;
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
    exBmpList.save(clean);
    ex_occ.save(clean);
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
    exBmpList.load();
    ex_occ.load();
    loadCooccur();
    itemAtt->buildKey2attMap();
    loadActTra();
}

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
}

void kgmod::Occ::expandItemByGranu(const size_t traNo, const string& traAttKey,
                                   Ewah& traFilter, Ewah& itemBmp) {
    string traAttVal;
    traAtt->traNo2traAtt(traNo, traAttKey, traAttVal);
    if (ex_occ.GetVal(traAttKey, traAttVal, itemBmp)) return;
    
    cerr << "calc traNo to transaction attribute bitmap: " << traAttKey << ":" << traAttVal << endl;
    Ewah granuBmp;
    bmpList.GetVal(traAttKey, traAttVal, granuBmp);
    granuBmp = granuBmp & traFilter;
    itemBmp.reset();
    for (auto t = granuBmp.begin(), et = granuBmp.end(); t != et; t++) {
        itemBmp = itemBmp | occ[*t];
    }
    ex_occ.InitKey(traAttKey, bmpList.getDataType(traAttKey));
    ex_occ.SetVal(traAttKey, traAttVal, itemBmp);
}

void kgmod::Occ::expandItemByGranu(const size_t traNo, const vector<string>& traAttKey,
                                   Ewah& traFilter, Ewah& itemBmp) {
    itemBmp.padWithZeroes(itemAtt->itemMax + 1);
    itemBmp.inplace_logicalnot();
    for (auto& k : traAttKey) {
        Ewah tmpBmp;
        expandItemByGranu(traNo, k, traFilter, tmpBmp);
        itemBmp = itemBmp & tmpBmp;
    }
}

size_t kgmod::Occ::itemFreq(const size_t itemNo, const Ewah& traFilter, const vector<string>* tra2key) {
    if (itemNo == 16438) {
        cerr << endl;
    }
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
    if (itemNo == 16438) {
        cerr << endl;
    }
    return bmpList[{_config->traFile.itemFld, itemAtt->item[itemNo]}].numberOfOnes();
}

size_t kgmod::Occ::attFreq(const vector<string>& attKeys, const vector<string>attVals,
                           const Ewah& traFilter, const Ewah& itemFilter, const vector<string>* tra2key) {
    if (attKeys[0] == "0011023580813") {
        cerr << endl;
    }
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
    traBmp = traBmp & traFilter;
    
    if (tra2key == NULL) {
        cnt = traBmp.numberOfOnes();
    } else {
        unordered_map<string, int> checkedAttVal;
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

size_t kgmod::Occ::attFreq(string& attKey, string& attVal, const Ewah& traFilter,
                           const Ewah& itemFilter, const vector<string>* tra2key) {
    if (attKey == "0011023580813") {
        cerr << endl;
    }
    Ewah *itemBmp;
    if (! itemAtt->bmpList.GetVal(attKey, attVal, itemBmp)) return 0;
    Ewah itemVals = *itemBmp & itemFilter;
//    Cmn::CheckEwah(itemVals);
    
    Ewah traBmp;
    for (auto i = itemVals.begin(); i != itemVals.end(); i++) {
        Ewah tmp;
        if (! bmpList.GetVal(_config->traFile.itemFld, itemAtt->item[*i], tmp)) continue;
        traBmp = traBmp | tmp;
    }
//    Cmn::CheckEwah(traBmp);
    traBmp = traBmp & traFilter;
//    Cmn::CheckEwah(traBmp);
    
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


void kgmod::Occ::dump(const bool debug) {
    if (! debug) return;
    
//    traAtt->dump(debug);
//    itemAtt->dump(debug);
    
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

void kgmod::Occ::getTra2KeyValue(vector<string>& keys, vector<string>& tra2key) {
    tra2key.resize(traAtt->traMax + 1);
    vector<string> vals;
    vector<Ewah> bmps;
    combiValues(keys, vals, bmps);
    for (size_t i = 0; i < vals.size(); i++) {
        for (auto t = bmps[i].begin(), et = bmps[i].end(); t != et; t++) {
            tra2key[*t] = vals[i];
        }
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
