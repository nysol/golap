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
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/optional.hpp>
#include <kgError.h>
#include <kgMethod.h>
#include <kgConfig.h>
#include <kgMessage.h>
#include "cmn.hpp"
#include "param.hpp"
#include "config.hpp"

using namespace kgmod;
using namespace kglib;

kgmod::Config::Config(string& infile) {
    if (infile.size() != 0) {
        Param Prm(infile);
        Prm.ReadParam();
        
        if (boost::optional<string> val = Prm.get<string>("dbDir")) {
            dbDir = *val;
        } else {
            throw kgError("dbDir is mandatory: " + infile);
        }
        if (boost::optional<string> val = Prm.get<string>("traFile.name")) {
            traFile.name = *val;
        } else {
            throw kgError("traFile.name is mandatory: " + infile);
        }
        if (boost::optional<string> val = Prm.get<string>("traFile.traFld")) {
            traFile.traFld = *val;
        } else {
            throw kgError("traFile.traFld is mandatory: " + infile);
        }
        if (boost::optional<string> val = Prm.get<string>("traFile.itemFld")) {
            traFile.itemFld = *val;
        } else {
            throw kgError("traFile.itemFld is mandatory: " + infile);
        }

        traDataType[traFile.traFld] = STR;
        itemDataType[traFile.itemFld] = STR;
        
        if (boost::optional<string> val = Prm.get<string>("traAttFile.name")) {
            traAttFile.name = *val;
        } else {
            throw kgError("traAttFile.name is mandatory: " + infile);
        }

        if (boost::optional<string> val = Prm.get<string>("traAttFile.strFields")) {
            traAttFile.strFields = Cmn::CsvStr::Parse(*val);
        }
        if (boost::optional<string> val = Prm.get<string>("traAttFile.numFields")) {
            traAttFile.numFields = Cmn::CsvStr::Parse(*val);
        }
        if (boost::optional<string> val = Prm.get<string>("traAttFile.catFields")) {
            traAttFile.catFields = Cmn::CsvStr::Parse(*val);
        }

        for (auto i = traAttFile.strFields.begin(); i != traAttFile.strFields.end(); i++) {
            traDataType[*i] = STR;
        }
        for (auto i = traAttFile.numFields.begin(); i != traAttFile.numFields.end(); i++) {
            traDataType[*i] = NUM;
        }
        for (auto i = traAttFile.catFields.begin(); i != traAttFile.catFields.end(); i++) {
            traDataType[*i] = STR;
        }
        
        if (boost::optional<string> val = Prm.get<string>("itemAttFile.name")) {
            itemAttFile.name = *val;
        } else {
            throw kgError("itemAttFile.name is mandatory: " + infile);
        }
        if (boost::optional<string> val = Prm.get<string>("itemAttFile.itemName")) {
            itemAttFile.itemName = *val;
        } else {
            throw kgError("itemAttFile.itemName is mandatory: " + infile);
        }
        if (boost::optional<string> val = Prm.get<string>("itemAttFile.strFields")) {
            itemAttFile.strFields = Cmn::CsvStr::Parse(*val);
        }
        if (boost::optional<string> val = Prm.get<string>("itemAttFile.numFields")) {
            itemAttFile.numFields = Cmn::CsvStr::Parse(*val);
        }
        if (boost::optional<string> val = Prm.get<string>("itemAttFile.catFields")) {
            itemAttFile.catFields = Cmn::CsvStr::Parse(*val);
        }
        
        for (auto i = itemAttFile.strFields.begin(); i != itemAttFile.strFields.end(); i++) {
            itemDataType[*i] = STR;
        }
        for (auto i = itemAttFile.numFields.begin(); i != itemAttFile.numFields.end(); i++) {
            itemDataType[*i] = NUM;
        }
        for (auto i = itemAttFile.catFields.begin(); i != itemAttFile.catFields.end(); i++) {
            itemDataType[*i] = STR;
        }
        
        if (boost::optional<string> val = Prm.get<string>("cmdCache.enable")) {
            string tmp = *val;
            transform(tmp.cbegin(), tmp.cend(), tmp.begin(), ::toupper);
            if (tmp == "TRUE") {
                cmdCache_enable = true;
            } else {
                cmdCache_enable = false;
            }
        } else {
            cmdCache_enable = false;
        }
        if (boost::optional<int> val = Prm.get<int>("cmdCache.cacheSize")) {
            cmdCache_size = *val;
        } else {
            cmdCache_size = 10;
        }
        if (boost::optional<int> val = Prm.get<int>("cmdCache.saveInterval")) {
            cmdCache_saveInterval = *val;
        } else {
            cmdCache_saveInterval = 1;
        }
        
        if (boost::optional<int> val = Prm.get<int>("etc.sendMax")) {
            sendMax = *val;
        } else {
            sendMax = 10000;
        }
        if (boost::optional<unsigned int> val = Prm.get<unsigned int>("etc.deadlineTimer")) {
            deadlineTimer = *val;
        } else {
            deadlineTimer = 60;
        }
        if (boost::optional<u_short> val = Prm.get<u_short>("etc.port")) {
            port = *val;
        } else {
            port = 31400;
        }

    } else {
        throw kgError("i= parameter is required");
    }
}

void kgmod::Config::dump(bool debug) {
    if (! debug) return;
    
    cerr << "<<< dump config >>>" << endl;
    cerr << "dbDir: " << dbDir << endl;
    cerr << "traFile: " << traFile.name << endl;
    cerr << "traFld: " << traFile.traFld << endl;
    cerr << "itemFld: " << traFile.itemFld << endl;
    cerr << "traAttFile: " << traAttFile.name << endl;
    cerr << "strFields: " ;
    for (auto i = traAttFile.strFields.begin(); i != traAttFile.strFields.end(); i++) {
        cerr << *i << " ";
    }
    cerr << endl;
    cerr << "numFidlds: ";
    for (auto i = traAttFile.numFields.begin(); i != traAttFile.numFields.end(); i++) {
        cerr << *i << " ";
    }
    cerr << endl;
    cerr << "catFields: ";
    for (auto i = traAttFile.catFields.begin(); i != traAttFile.catFields.end(); i++) {
        cerr << *i << " ";
    }
    cerr << endl;
    cerr << "itemAttFile: " << itemAttFile.name << endl;
    cerr << "itemName: " << itemAttFile.itemName << endl;
    cerr << "strFidls: ";
    for (auto i = itemAttFile.strFields.begin(); i != itemAttFile.strFields.end(); i++) {
        cerr << *i << " ";
    }
    cerr << endl;
    cerr << "numFilds: ";
    for (auto i = itemAttFile.numFields.begin(); i != itemAttFile.numFields.end(); i++) {
        cerr << *i << " ";
    }
    cerr << endl;
    cerr << "catFields: ";
    for (auto i = itemAttFile.catFields.begin(); i != itemAttFile.catFields.end(); i++) {
        cerr << *i << " ";
    }
    cerr << endl;
    
    cerr << "traDataType: ";
    for (auto i = traDataType.begin(); i != traDataType.end(); i++) {
        cerr << "(" << i->first << ":" << DataTypeStr[i->second] << ") ";
    }
    cerr << endl;
    
    cerr << "itemDataType: ";
    for (auto i = itemDataType.begin(); i != itemDataType.end(); i++) {
        cerr << "(" << i->first << ":" << DataTypeStr[i->second] << ") ";
    }
    cerr << endl;
    
    cerr << "etc.sendMax: " << sendMax <<endl;
    cerr << "etc.daedlineTimer: " << deadlineTimer <<endl;
    cerr << "etc.port: " << port <<endl;
}