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

kgmod::itemAtt::itemAtt(Config* config, kgEnv* env) : config(config), env(env), itemMax(-1) {
    this->dbName = config->dbDir + "/itematt.dat";
    string dtmDb = config->dbDir + "/item.dtm";
    string itmDb = config->dbDir + "/item.dat";
    bmpList.PutDbName(dtmDb, itmDb);
}

void kgmod::itemAtt::build(void) {
    kgCSVfld itemAttF;
    itemAttF.open(config->itemAttFile.name, env, false);
    itemAttF.read_header();
    vector<string> fldName = itemAttF.fldName();
    
    cerr << "<<< build itemAtt >>>" << endl;
    auto itemFld = find(fldName.begin(), fldName.end(), config->traFile.itemFld);
    if (itemFld == fldName.end()) {
        stringstream msg;
        msg << config->traFile.itemFld <<" not found in " << config->itemAttFile.name;
        throw kgError(msg.str());
    }
    int itemFldPos = (int)(itemFld - fldName.begin());
    
    itemMax = -1;
    size_t fldCnt = fldName.size();
    vector<bool> isFirst(fldCnt, true);
    while (itemAttF.read() != EOF) {
        itemMax++;
        string val = itemAttF.getVal(itemFldPos);
        itemNo[val] = itemMax;
//        cerr << val << "," << itemNo[val] << " ";
        for (auto fld = fldName.begin(); fld != fldName.end(); fld++) {
//            cerr << *fld;
            int cnt = (int)(fld - fldName.begin());
//            if (cnt == itemFldPos) continue;
            string val1 = itemAttF.getVal(cnt);
//            if (val1 == "") continue;
            if (isFirst[cnt]) {
                bmpList.InitKey(*fld, config->itemDataType[*fld]);
                isFirst[cnt] = false;
            }
            bmpList.SetBit(*fld, val1, itemMax);
            
            if (*fld == config->itemAttFile.itemName) {
                itemName[itemMax] = val1;
            }
        }
    }
    itemAttF.close();
    cerr << endl;

    for (auto i = itemNo.begin(); i != itemNo.end(); i++) {
        item[i->second] = i->first;
    }
}

void kgmod::itemAtt::save(bool clean) {
    cerr << "saving item attributes" << endl;
    bmpList.save(clean);
    
    cerr << "writing " << dbName << " ..." << endl;
    if (clean) {
        ofstream osf(dbName,ios::out);
        if (osf.fail()) {
            stringstream msg;
            msg << "failed to open " + dbName;
            throw kgError(msg.str());
        }
        osf.close();
    }
    
    ofstream ofs(dbName, ios::app);
    if (ofs.fail()) {
        stringstream msg;
        msg << "failed to open " + dbName;
        throw kgError(msg.str());
    }
    try {
        for (size_t i = 0; i <= itemMax; i++) {
            ofs << item[i] << "," << itemName[i] << endl;
        }
    } catch(...) {
        ofs.close();
        stringstream msg;
        msg << "failed to read " << dbName;
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
    ifstream ifs(dbName);
    if (ifs.fail()) {
        stringstream msg;
        msg << "failed to open " + dbName;
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
        msg << "failed to read " + dbName;
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
    //    vector<string> out;
    //    size_t siz = config->itemAttFile.numFields.size();
    //    siz += config->itemAttFile.strFields.size();
    //    siz += config->itemAttFile.catFields.size();
    //    out.reserve(siz);
    
    vector<string> out = config->itemAttFile.numFields;
    out.insert(out.end(), config->itemAttFile.strFields.begin(), config->itemAttFile.strFields.end());
    out.insert(out.end(), config->itemAttFile.catFields.begin(), config->itemAttFile.catFields.end());
    sort(out.begin(), out.end());
    return out;
}
