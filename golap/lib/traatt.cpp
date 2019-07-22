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
#include <kgError.h>
#include <kgMethod.h>
#include <kgConfig.h>
#include <kgMessage.h>
#include "kgCSV.h"
#include "btree_map.h"
#include "btree.hpp"
#include "bidx-ewah.hpp"
#include "cmn.hpp"
#include "traatt.hpp"

using namespace std;
using namespace kgmod;

kgmod::TraAtt::TraAtt(Config* config, kgEnv* env): _config(config), _env(env), traMax(-1) {
    this->_dbName = config->dbDir + "/traatt.dat";
}

void kgmod::TraAtt::build(BTree& bmpList) {
    kgCSVfld traAttF;
    traAttF.open(_config->traAttFile.name, _env, false);
    traAttF.read_header();
    vector<string> fldName = traAttF.fldName();
    
    cerr << "building transaction attribute index" << endl;
    auto traFld = find(fldName.begin(), fldName.end(), _config->traFile.traFld);
    if (traFld == fldName.end()) {
        stringstream msg;
        msg << _config->traFile.traFld <<" not found in " << _config->traFile.name;
        throw kgError(msg.str());
    }
    int traFldPos = (int)(traFld - fldName.begin());
    
    const size_t stlipEvery = 10000;
    size_t fldCnt = fldName.size();
    int fstat = EOF + 1;    // EOFでない値で初期化
//    typedef btree::btree_map<pair<string, size_t>, Ewah> res;
    typedef btree::btree_map<pair<string, string>, Ewah> res;
    typedef btree::btree_map<size_t, res*> res_list;
    res_list* resList;
    resList = new res_list;
    bool isError = false;
    size_t split;
    for (split = 0; fstat != EOF; split++) {
        (*resList)[split] = new res;
        vector<bool> isFirst(fldCnt, true);
        while ((fstat = traAttF.read()) != EOF) {
            string traVal = traAttF.getVal(traFldPos);
            if (traNo.find(traVal) != traNo.end()) {
                stringstream ss;
                ss << "#ERROR# " << _config->traFile.traFld << ":" << traVal << " is not unique on " << _config->traAttFile.name;
                cerr << ss.str() << endl;
                isError = true;
                continue;
            }
            traNo[traVal] = traAttF.recNo() - 1;
            traMax = traAttF.recNo() - 1;
            
            for (auto fld = fldName.begin(); fld != fldName.end(); fld++) {
                int cnt = (int)(fld - fldName.begin());
                if (cnt == traFldPos) continue;
                string fldVal = traAttF.getVal(cnt);
                
                if (isFirst[cnt]) {
                    Ewah bmp;
                    bmp.set(traMax);
                    (*(*resList)[split])[{*fld, fldVal}] = bmp;
                    isFirst[cnt] = false;
                } else {
                    (*(*resList)[split])[{*fld, fldVal}].set(traMax);
                }
                
                if (traMax % stlipEvery == stlipEvery - 1) break;
            }
        }
    }
    traAttF.close();
    if (isError) throw kgError("error occuerred in building transaction attrbute index");

    // gathering result
    while (resList->size() > 1) {
        res_list* tmp = new res_list;
        for (auto split = resList->begin(); split != resList->end(); split++) {
            auto split1 = split;
            split++;
            auto split2 = split;
            if (split2 == resList->end()) split--;
            
            (*tmp)[split->first] = new res;
            
            for (auto fld = fldName.begin(); fld != fldName.end(); fld++) {
                auto s1 = split1->second->lower_bound({*fld, ""});
                auto s2 = split2->second->lower_bound({*fld, ""});
                while (s1 != split1->second->end() | s2 != split2->second->end()) {
                    if (s1->first.second == s2->first.second) {   // val
                        (*(*tmp)[split->first])[{s1->first}] = s1->second | s2->second;
                        s1++;
                        s2++;
                    } else if (s1 != split1->second->end() | s1->first.second > s2->first.second) {
                        (*(*tmp)[split->first])[{s2->first}] = s2->second;
                        s2++;
                    } else if (s2 != split2->second->end() | s1->first.second < s2->first.second) {
                        (*(*tmp)[split->first])[{s1->first}] = s1->second;
                        s1++;
                    }
                }
            }
        }
        
        for (auto i = resList->begin(); i != resList->end(); i++) {
            delete i->second;
        }
        delete resList;
        resList = tmp;
    }
    
    for (auto split = resList->begin(); split != resList->end(); split++) {
        for (auto fld = split->second->begin(); fld != split->second->end(); fld++) {
            bmpList.InitKey(fld->first.first, _config->traDataType[fld->first.first]);
            bmpList.SetVal(fld->first.first, fld->first.second, fld->second);
        }
    }
    delete resList;
    
    for (auto i = traNo.begin(); i != traNo.end(); i++) {
        tra[i->second] = i->first;
    }
}

void kgmod::TraAtt::save(bool clean) {
    cerr << "writing " << _dbName << " ..." << endl;
    if (clean) {
        ofstream osf(_dbName,ios::out);
        osf.close();
    }
    
    ofstream ofs(_dbName, ios::app);
    if (ofs.fail()) {
        stringstream msg;
        msg << "failed to open " + _dbName;
        throw kgError(msg.str());
    }
    try {
        for (auto i = tra.begin(); i != tra.end(); i++) {
            ofs << i->second << endl;
        }
    } catch(...) {
        ofs.close();
        stringstream msg;
        msg << "failed to read " << _dbName;
        throw kgError(msg.str());
    }
    ofs.close();
}

void kgmod::TraAtt::load(void) {
    cerr << "loading transaction attributes" << endl;
    tra.clear();
    traNo.clear();
    
    string tr;
    size_t i = -1;
    ifstream ifs(_dbName);
    if (ifs.fail()) {
        stringstream msg;
        msg << "failed to open " + _dbName;
        throw kgError(msg.str());
    }
    try {
        while (getline(ifs, tr)) {
            i++;
            tra[i] = tr;
            traNo[tr] = i;
        }
        traMax = i;
    } catch(...) {
        ifs.close();
        stringstream msg;
        msg << "failed to read " << _dbName;
        throw kgError(msg.str());
    }
    ifs.close();
}

void kgmod::TraAtt::dump(bool debug) {
    if (! debug) return;
    
    cerr << "<<< dump traAtt >>>" << endl;
/*****************
    cerr << "traNo: ";
    for (auto i = traNo.begin(); i != traNo.end(); i++) {
        cerr << "{" << i->first << "," << i->second << "} ";
    }
    cerr << endl;
    
    cerr << "tra: ";
    for (auto i = tra.begin(); i != tra.end(); i++) {
        cerr << "{" << i->first << "," << i->second << "} ";
    }
    cerr << endl;
*********************/
    cerr << "traMax: " << traMax << endl;
    cerr << "*** see 'dump occ' if you check traAtt index ***" << endl;
    cerr << endl;
}

vector<string> kgmod::TraAtt::listAtt(void) {
    vector<string> out = _config->traAttFile.numFields;
    out.insert(out.end(), _config->traAttFile.strFields.begin(), _config->traAttFile.strFields.end());
    out.insert(out.end(), _config->traAttFile.catFields.begin(), _config->traAttFile.catFields.end());
    sort(out.begin(), out.end());
    return out;
}
