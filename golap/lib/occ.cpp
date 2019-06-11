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
#include "kgCsv.h"
#include "btree.hpp"
#include "bidx-ewah.hpp"
#include "cmn.hpp"
#include "occ.hpp"
#include "traatt.hpp"

using namespace std;
using namespace kgmod;

kgmod::Occ::Occ(Config* config, kgEnv* env) : config(config), env(env) {
    this->dbName = config->dbDir + "/coitem.dat";
    
    string dtmDb = config->dbDir + "/occ.dtm";
    string occDb = config->dbDir + "/occ.dat";
    bmpList.PutDbName(dtmDb, occDb);
    
    traAtt = new class traAtt(config, env);
    itemAtt = new class itemAtt(config, env);
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
    
    cerr << "<<< build transaction >>>" << endl;
    kgCSVfld tra;
    tra.open(config->traFile.name, env, false);
    tra.read_header();
    vector<string> fldName = tra.fldName();
    string traNameFld;
    int traNameFldPos = -1;
    string itemNameFld;
    int itemNameFldPos = -1;
    for (int i = 0; i < fldName.size(); i++) {
        if (fldName[i] == config->traFile.traFld) {
            traNameFld = config->traFile.traFld;
            traNameFldPos = i;
        } else if (fldName[i] == config->traFile.itemFld) {
            traNameFld = config->traFile.itemFld;
            itemNameFldPos = i;
        }
    }
    if (traNameFldPos == -1 || itemNameFldPos == -1) {
        stringstream ss;
        ss << "error in " << config->traFile.name;
        throw new Cmn::Exception(ss.str());
    }
    
    while (tra.read() != EOF) {
        string traName = tra.getVal(traNameFldPos);
        string itemName = tra.getVal(itemNameFldPos);
        
        bmpList.SetBit(occKey, itemName, traAtt->traNo[traName]);
        if (traAtt->traNo[traName] >= occ.size()) occ.resize(traAtt->traNo[traName] + 1);
        occ[traAtt->traNo[traName]].set(itemAtt->itemNo[itemName]);
    }
    tra.close();
}

void kgmod::Occ::saveCooccur(const bool clean) {
//    cerr << occ.size() << endl;
    if (clean) {
        FILE* fp = fopen(dbName.c_str(), "wb");
        fclose(fp);
    }

    cerr << "writing " << dbName << " ..." << endl;
    FILE* fp = fopen(dbName.c_str(), "ab");
    if (fp == NULL) {
        stringstream msg;
        msg << "failed to open " + dbName;
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
        msg << "failed to write " << dbName;
        throw kgError(msg.str());
    }
    fclose(fp);
}

void kgmod::Occ::save(const bool clean) {
    traAtt->save(clean);
    itemAtt->save(clean);
    bmpList.save(clean);
    saveCooccur(clean);
}

void kgmod::Occ::loadCooccur(void) {
    cerr << "reading co-occurrence" << endl;
    occ.resize(traAtt->traMax + 1);
    
    FILE* fp = fopen(dbName.c_str(), "rb");
    if (fp == NULL) {
        stringstream msg;
        msg << "failed to open " + dbName;
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
        msg << "failed to read " << dbName;
        throw kgError(msg.str());
    }
    
    fclose(fp);
}

void kgmod::Occ::load(void) {
    traAtt->load();
    itemAtt->load();
    bmpList.load();
    loadCooccur();
}

void kgmod::Occ::dump(const bool debug) {
    if (! debug) return;
    
    traAtt->dump(debug);
    itemAtt->dump(debug);
    
    cerr << "<<< dump occ>>>" << endl;
    bmpList.dump(debug);
    cerr << endl;
    
/********
    for (size_t i = 0; i < occ.size(); i++) {
        cerr << "occ[" << i << "] ";
        Cmn::CheckEwah(occ[i]);
    }
 *******/
}
