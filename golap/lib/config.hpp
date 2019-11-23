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
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/optional.hpp>
#include "btree_map.h"
#include "param.hpp"

using namespace std;

namespace kgmod {
    // STR_HC,NUM_HCではhigh cardinality用のbitmapを使わないインデックスを使用する
    enum DataType {NONE=0,STR=1,NUM=2,STR_HC=3,NUM_HC=4};
    const vector<string> DataTypeStr = {"NONE","STR","NUM","STR_HC","NUM_HC"};
    
    struct Config {
        Param Prm;
        
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
            vector<string> highCardinality;
            vector<string> granuFields;
        } traAttFile;
        btree::btree_map<string, DataType> traDataType; // ["field name"] -> data type
        
        struct itemAttFile {
            string name;
            string itemName;
            vector<string> strFields;
            vector<string> numFields;
            vector<string> highCardinality;
            string imageField;
            unordered_map<string, string> code2name_map;
            unordered_map<string, string> name2code_map;
        } itemAttFile;
        btree::btree_map<string, DataType> itemDataType; // ["field name"] -> data type
        
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
        bool getJson(string& json) {return Prm.convJson(json);}
        void dump(bool debug);
    };
}

#endif /* config_hpp */
