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

#ifndef config_hpp
#define config_hpp

#include <iostream>
#include <unordered_map>
#include <vector>
#include "btree_map.h"

using namespace std;

namespace kgmod {
    enum DataType {NONE=0,STR=1,NUM=2,CAT=3};
    const vector<string> DataTypeStr = {"NONE","STR","NUM","CAT"};
    
    struct Config {
        string dbDir;
        string outDir;
        
        struct traFile {
            string name;
            string traFld;
            string itemFld;
        } traFile;
        
        struct traAttFile {
            string name;
            vector<string> strFields;
            vector<string> numFields;
            vector<string> catFields;
            vector<string> granuFields;
        } traAttFile;
        btree::btree_map<string, DataType> traDataType;
        
        struct itemAttFile {
            string name;
            string itemName;
            vector<string> strFields;
            vector<string> numFields;
            vector<string> catFields;
            string imageField;
            unordered_map<string, string> code2name_map;
            unordered_map<string, string> name2code_map;
        } itemAttFile;
        btree::btree_map<string, DataType> itemDataType;
        
        bool cmdCache_enable;
        size_t cmdCache_size;
        size_t cmdCache_saveInterval;
        
#define MAX_THREAD 100
        bool mt_enable;
        size_t mt_degree;
        
        size_t bottomSupport;
        
        size_t sendMax;
        u_short port;
        unsigned int deadlineTimer;
        
    public:
        Config(string& infile);
        void dump(bool debug);
    };
}

#endif /* config_hpp */
