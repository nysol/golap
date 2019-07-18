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

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include "kgCSV.h"
#include "btree.hpp"
#include "bidx-ewah.hpp"
#include "cmn.hpp"
#include "itematt.hpp"

using namespace std;
using namespace kgmod;

kgmod::itemAtt::itemAtt(Config* config, kgEnv* env) : _config(config), _env(env), itemMax(-1) {
    this->_dbName = _config->dbDir + "/itematt.dat";
    string dtmDb = _config->dbDir + "/item.dtm";
    string itmDb = _config->dbDir + "/item.dat";
    bmpList.PutDbName(dtmDb, itmDb);
}

void kgmod::itemAtt::build(void) {
    kgCSVfld itemAttF;
    itemAttF.open(_config->itemAttFile.name, _env, false);
    itemAttF.read_header();
    vector<string> fldName = itemAttF.fldName();
    
    cerr << "building item attribute index" << endl;
    auto itemFld = find(fldName.begin(), fldName.end(), _config->traFile.itemFld);
    if (itemFld == fldName.end()) {
        stringstream msg;
        msg << _config->traFile.itemFld <<" not found in " << _config->itemAttFile.name;
        throw kgError(msg.str());
    }
    int itemFldPos = (int)(itemFld - fldName.begin());
    
    itemMax = -1;
    size_t fldCnt = fldName.size();
    vector<bool> isFirst(fldCnt, true);
    bool isError = false;
    while (itemAttF.read() != EOF) {
        itemMax++;
        string val = itemAttF.getVal(itemFldPos);
        if (itemNo.find(val) != itemNo.end()) {
            stringstream ss;
            ss << "#ERROR# " << _config->traFile.itemFld << ":" << val << " is not unique on " << _config->itemAttFile.name;
            cerr << ss.str() << endl;
            isError = true;
            continue;
        }
        itemNo[val] = itemMax;
        for (auto fld = fldName.begin(); fld != fldName.end(); fld++) {
            int cnt = (int)(fld - fldName.begin());
            string val1 = itemAttF.getVal(cnt);
            if (isFirst[cnt]) {
                bmpList.InitKey(*fld, _config->itemDataType[*fld]);
                isFirst[cnt] = false;
            }
            bmpList.SetBit(*fld, val1, itemMax);
            
            if (*fld == _config->itemAttFile.itemName) {
                itemName[itemMax] = val1;
            }
        }
    }
    itemAttF.close();
    if (isError) throw kgError("error occuerred in building item attrbute index");

    for (auto i = itemNo.begin(); i != itemNo.end(); i++) {
        item[i->second] = i->first;
    }
}

void kgmod::itemAtt::save(bool clean) {
    bmpList.save(clean);
    
    cerr << "writing " << _dbName << " ..." << endl;
    if (clean) {
        ofstream osf(_dbName,ios::out);
        if (osf.fail()) {
            stringstream msg;
            msg << "failed to open " + _dbName;
            throw kgError(msg.str());
        }
        osf.close();
    }
    
    ofstream ofs(_dbName, ios::app);
    if (ofs.fail()) {
        stringstream msg;
        msg << "failed to open " + _dbName;
        throw kgError(msg.str());
    }
    try {
        for (size_t i = 0; i <= itemMax; i++) {
            ofs << item[i] << "," << itemName[i] << endl;
        }
    } catch(...) {
        ofs.close();
        stringstream msg;
        msg << "failed to read " << _dbName;
        throw kgError(msg.str());
    }
    ofs.close();
}

void kgmod::itemAtt::load(void) {
    cerr << "loading item attributes" << endl;
    bmpList.load();
    
    item.clear();
    itemNo.clear();
    
    string it;
    size_t i = -1;
    ifstream ifs(_dbName);
    if (ifs.fail()) {
        stringstream msg;
        msg << "failed to open " + _dbName;
        throw kgError(msg.str());
    }
    try {
        while (getline(ifs, it)) {
            vector<string> itv(2);
            itv = Cmn::CsvStr::Parse(it);
            i++;
            item[i] = itv[0];
            itemNo[itv[0]] = i;
            itemName[i] = itv[1];
        }
        itemMax = i;
    } catch(...) {
        ifs.close();
        stringstream msg;
        msg << "failed to read " + _dbName;
        throw kgError(msg.str());
    }
    ifs.close();
}

void kgmod::itemAtt::dump(bool debug) {
    if (! debug) return;
    
    cerr << "<<< dump itemAtt >>>" << endl;
    cerr << "itemNo: ";
    for (auto i = itemNo.begin(); i != itemNo.end(); i++) {
        cerr << "{" << i->first << "," << i->second << "} ";
    }
    cerr << endl;

    cerr << "item: ";
    for (auto i = item.begin(); i != item.end(); i++) {
        cerr << "{" << i->first << "," << i->second << "} ";
    }
    cerr << endl;
    
    cerr << "itemName: ";
    for (auto i = itemName.begin(); i != itemName.end(); i++) {
        cerr << "{" << i->first << "," << i->second << "} ";
    }

    cerr << "itemMax: " << itemMax << endl;
    
    bmpList.dump(debug);
    cerr << endl;
}

vector<string> kgmod::itemAtt::listAtt(void) {
    vector<string> out = _config->itemAttFile.numFields;
    out.insert(out.end(), _config->itemAttFile.strFields.begin(), _config->itemAttFile.strFields.end());
    out.insert(out.end(), _config->itemAttFile.catFields.begin(), _config->itemAttFile.catFields.end());
    sort(out.begin(), out.end());
    return out;
}

void kgmod::itemAtt::buildKey2attMap(void) {
    cerr << "making key2att map" << endl;
    vector<string> attName = listAtt();
    for (auto k = attName.begin(), ek = attName.end(); k != ek; k++) {
        BTree::kvHandle* kvs = NULL;
        pair<string, Ewah> res = bmpList.GetAllKeyValue(*k, kvs);
        while (kvs != NULL) {
            for (auto i = res.second.begin(), ei = res.second.end(); i != ei; i++) {
                key2att_map[{*i, *k}] = res.first;
            }
            res = bmpList.GetAllKeyValue(*k, kvs);
        }
    }
}

void kgmod::itemAtt::dumpKey2attMap(bool debug) {
    if (! debug) return;
    
    cerr << "<<< key2att map >>>" << endl;
    for (auto i = key2att_map.begin(), ei = key2att_map.end(); i != ei; i++) {
        cerr << item[i->first.first] << "(" << i->first.first << ")," << i->first.second << "->" << i->second << endl;
    }
}
