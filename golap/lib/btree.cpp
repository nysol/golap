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
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <limits.h>
#include <kgError.h>
#include <kgMethod.h>
#include <kgConfig.h>
#include <kgMessage.h>
#include <cfloat>
#include "btree.hpp"
#include "bidx-ewah.hpp"
#include "cmn.hpp"

using namespace std;
using namespace kgmod;

void kgmod::BTree::PutDbName(const string dataTypeMapDb_, const string dbName_) {
    dataTypeMapDb = dataTypeMapDb_;
    dbName = dbName_;
}

void kgmod::BTree::InitKey(const string& Key, const DataType& DataType) {
    DataTypeMap[Key] = DataType;
}

void kgmod::BTree::SetBit(const string& Key, const string& KeyValue, size_t bit) {
    if (DataTypeMap[Key] == STR) {
        if (str_btree.find({Key,KeyValue}) != str_btree.end()) {
            // found in btree
            if (str_btree[{Key, KeyValue}].sizeInBits() - 1 == bit) {
                // noop
                // データの重複分はエラー扱いにしない
            } else if (str_btree[{Key, KeyValue}].sizeInBits() > bit) {
                stringstream msg;
                msg << "invalid set bit: " << str_btree[{Key, KeyValue}].sizeInBits() << " > " << bit;
                throw kgError(msg.str());
            } else {
                str_btree[{Key,KeyValue}].set(bit);
            }
        } else {
            // not found in btree
            Ewah tmp;
            tmp.set(bit);
            str_btree[{Key,KeyValue}] = tmp;
        }
    } else if (DataTypeMap[Key] == NUM) {
        double dbl;
        if (KeyValue.length() == 0) {
            dbl = -DBL_MAX;
        } else {
            dbl = stod(KeyValue);
        }
        num_str[dbl] = KeyValue;
        if (num_btree.find({Key, dbl}) != num_btree.end()) {
            // found in btree
            if (num_btree[{Key, dbl}].sizeInBits() > bit) {
                // noop
                // データの重複分はエラー扱いにしない
            } else if (num_btree[{Key, dbl}].sizeInBits() > bit) {
                stringstream msg;
                msg << "invalid set bit: " << num_btree[{Key, dbl}].sizeInBits() << " > " << bit;
                throw kgError(msg.str());
            } else {
                num_btree[{Key, dbl}].set(bit);
//                Cmn::CheckEwah(num_btree[{Key, dbl}]);
            }
        } else {
            // not found in btree
            Ewah tmp;
            tmp.set(bit);
            num_btree[{Key, dbl}] = tmp;
        }
    }
}

void kgmod::BTree::SetVal(const string& Key, const string& KeyValue, Ewah& Bitmap) {
    if (DataTypeMap[Key] == STR) {
        str_btree[{Key, KeyValue}] = Bitmap;
    } else if (DataTypeMap[Key] == NUM) {
        double dbl;
        if (KeyValue.length() == 0) {
            dbl = -DBL_MAX;
        } else {
            dbl = stod(KeyValue);
        }
        num_btree[{Key,dbl}] = Bitmap;
        num_str[dbl] = KeyValue;
    }
}

bool kgmod::BTree::GetVal(const string& Key, const string& KeyValue, Ewah*& Bitmap) {
    bool found = false;
    static Ewah nullBmp;
    Bitmap = &nullBmp;
    if (DataTypeMap[Key] == STR) {
        if (str_btree.find({Key,KeyValue}) != str_btree.end()) {
            Bitmap = &str_btree[{Key,KeyValue}];
            found = true;
        }
    } else if (DataTypeMap[Key] == NUM) {
        double dbl;
        if (KeyValue.length() == 0) {
            dbl = -DBL_MAX;
        } else {
            dbl = stod(KeyValue);
        }
        if (num_btree.find({Key,dbl}) != num_btree.end()) {
            Bitmap = &num_btree[{Key, dbl}];
            found = true;
        }
    }
    return found;
}

bool kgmod::BTree::GetVal(const string& Key, const string& KeyValue, Ewah& Bitmap) {
    Ewah* tmp = NULL;
    bool ret = GetVal(Key, KeyValue, tmp);
    Bitmap = *tmp;
    return ret;
}

Ewah& kgmod::BTree::GetVal(const string& Key, const string& KeyValue) {
    static Ewah Zero; Zero.reset();
    if (DataTypeMap[Key] == STR) {
//        if (str_btree.find({Key, KeyValue}) == str_btree.end()) str_btree[{Key, KeyValue}] = Zero;
        if (str_btree.find({Key, KeyValue}) == str_btree.end()) {
            return Zero;
        } else {
            return str_btree[{Key, KeyValue}];
        }
    } else if (DataTypeMap[Key] == NUM) {
        double dbl;
        if (KeyValue.length() == 0) {
            dbl = -DBL_MAX;
        } else {
            dbl = stod(KeyValue);
        }
//        if (num_btree.find({Key, dbl}) == num_btree.end()) num_btree[{Key, dbl}] = Zero;
        if (num_btree.find({Key, dbl}) == num_btree.end()) {
            return Zero;
        } else {
            return num_btree[{Key, dbl}];
        }
    } else {
        stringstream msg;
//        msg << "invalid DataTypeMap: " << DataTypeMap[Key];
//        cerr << msg.str() << endl;
//        msg.clear();
        msg << Key << " is not found";
        throw kgError(msg.str());
    }
}

bool kgmod::BTree::GetValMulti(const string& Key, const string& LikeKey, Ewah& Bitmap) {
    if (DataTypeMap[Key] != STR) return false;
    size_t FirstWild = min(LikeKey.find("*"), LikeKey.find("?"));
    string StartWith = LikeKey.substr(0, FirstWild);
    string EndOfSearch = StartWith;
    if (FirstWild != 0) EndOfSearch[FirstWild - 1]++;
    
    Bitmap.reset();
    auto s = (FirstWild == 0) ? str_btree.begin() : str_btree.lower_bound({Key,StartWith});
    auto e = (FirstWild == 0) ? str_btree.end() : str_btree.upper_bound({Key,EndOfSearch});
    for (auto i = s; i != e; i++) {
        if (i->first.first != Key) continue;
        if (Cmn::MatchWild(LikeKey.c_str(), i->first.second.c_str())) {
            Bitmap = Bitmap | i->second;
        }
    }
    return true;
}

bool kgmod::BTree::GetValMulti(const string& Key, const string& Kakko, const string& FromKey,
                               const string& Kokka, const string& ToKey, Ewah& Bitmap) {
    Bitmap.reset();
    try {
        if (DataTypeMap[Key] == STR) {
            for (auto i = str_btree.lower_bound({Key,FromKey}); i != str_btree.upper_bound({Key,ToKey}); i++) {
                if ((Kakko == "(") && (i->first.second == FromKey)) continue;
                if ((Kokka == ")") && (i->first.second == ToKey)) continue;
                Bitmap = Bitmap | i->second;
            }
        } else if (DataTypeMap[Key] == NUM) {
            double DblFromKey;
            if (FromKey.length() == 0) {
                DblFromKey = -DBL_MAX;
            } else {
                DblFromKey = stod(FromKey);
            }
            double DblToKey;
            if (ToKey.length() == 0) {
                DblToKey   = DBL_MAX;
            } else {
                DblToKey   = stod(ToKey);
            }
            for (auto i = num_btree.lower_bound({Key,DblFromKey}); i != num_btree.upper_bound({Key,DblToKey}); i++) {
                if ((Kakko == "(") && (i->first.second == DblFromKey)) continue;
                if ((Kokka == ")") && (i->first.second == DblToKey)) continue;
                if (i->first.second == -DBL_MAX) continue;
                Bitmap = Bitmap | i->second;
            }
        } else {
            return false;
        }
    } catch (exception& e) {
        cerr << "#ERROR# ; Data conversion error" << endl;
        return false;
    }
    return true;
}

bool kgmod::BTree::GetValMulti(const string& Key, const string& Operator, const string& KeyValue, Ewah& Bitmap) {
    Bitmap.reset();
    if (Operator == "(" || Operator == "[") {
        try {
            if (DataTypeMap[Key] == STR) {
                for (auto i = str_btree.lower_bound({Key,KeyValue}); i != str_btree.end(); i++) {
                    if ((Operator == "(") && (i->first.second == KeyValue)) continue;
                    Bitmap = Bitmap | i->second;
                }
            } else if (DataTypeMap[Key] == NUM) {
                double DblKeyValue;
                if (KeyValue.length() == 0) {
                    DblKeyValue = -DBL_MAX;
                } else {
                    DblKeyValue = stod(KeyValue);
                }
                for (auto i = num_btree.lower_bound({Key,DblKeyValue}); i != num_btree.end(); i++) {
                    if ((Operator == "(") && (i->first.second == DblKeyValue)) continue;
                    if (i->first.second == -DBL_MAX) continue;
                    Bitmap = Bitmap | i->second;
                }
            }
        } catch (exception& e) {
            cerr << "#ERROR# ; Data conversion error" << endl;
            return false;
        }
    } else if (Operator == ")" || Operator == "]") {
        try {
            if (DataTypeMap[Key] == STR) {
                for (auto i = str_btree.begin(); i != str_btree.upper_bound({Key,KeyValue}); i++) {
                    if ((Operator == ")") && (i->first.second == KeyValue)) continue;
                    Bitmap = Bitmap | i->second;
                }
            } else if (DataTypeMap[Key] == NUM) {
                double DblKeyValue;
                if (KeyValue.length() == 0) {
                    DblKeyValue = DBL_MAX;
                } else {
                    DblKeyValue = stod(KeyValue);
                }
                for (auto i = num_btree.begin() ; i != num_btree.upper_bound({Key,DblKeyValue}); i++) {
                    if ((Operator == ")") && (i->first.second == DblKeyValue)) continue;
                    if (i->first.second == -DBL_MAX) continue;
                    Bitmap = Bitmap | i->second;
                }
            }
        } catch (exception& e) {
            cerr << "#ERROR# ; Data conversion error" << endl;
            return false;
        }
    } else {
        return false;
    }
    return true;
}

void kgmod::BTree::GetAllKeyValue(const string& Key, pair<string, Ewah>& out, kvHandle*& kvh) {
    out.first = "";
    out.second.reset();
    
    if (kvh == NULL) {
        kvh = new kvHandle;
        if (DataTypeMap[Key] == STR) {
            kvh->str_iter = str_btree.lower_bound({Key, ""});
            if (kvh->str_iter == str_btree.end()) {
                delete kvh;
                kvh = NULL;
            } else if (kvh->str_iter->first.first == Key) {
                out.first  = kvh->str_iter->first.second;
                out.second = kvh->str_iter->second;
            } else {
                delete kvh;
                kvh = NULL;
            }
        } else if (DataTypeMap[Key] == NUM) {
            kvh->num_iter = num_btree.lower_bound({Key, -DBL_MAX});
            if (kvh->num_iter == num_btree.end()) {
                delete kvh;
                kvh = NULL;
            } else if (kvh->num_iter->first.first == Key) {
                out.first  = num_str[kvh->num_iter->first.second];
                out.second = kvh->num_iter->second;
            } else {
                delete kvh;
                kvh = NULL;
            }
        } else {
            delete kvh;
            kvh = NULL;
        }
    } else {
        if (DataTypeMap[Key] == STR) {
            kvh->str_iter++;
            if (kvh->str_iter == str_btree.end()) {
                delete kvh;
                kvh = NULL;
            } else if (kvh->str_iter->first.first == Key) {
                out.first  = kvh->str_iter->first.second;
                out.second = kvh->str_iter->second;
            } else {
                delete kvh;
                kvh = NULL;
            }
        } else if (DataTypeMap[Key] == NUM) {
            kvh->num_iter++;
            if (kvh->num_iter == num_btree.end()) {
                delete kvh;
                kvh = NULL;
            } else if (kvh->num_iter->first.first == Key) {
                out.first  = num_str[kvh->num_iter->first.second];
                out.second = kvh->num_iter->second;
            } else {
                delete kvh;
                kvh = NULL;
            }
        } else {
            delete kvh;
            kvh = NULL;
        }
    }
}

void kgmod::BTree::save(bool clean) {
    cerr << "writing " << dataTypeMapDb << " ..." << endl;
    if (clean) {
        ofstream ofs(dataTypeMapDb);
        if (ofs.fail()) {
            stringstream msg;
            msg << "failed to open " + dataTypeMapDb;
            throw kgError(msg.str());
        }
        ofs.close();
    }
    
    ofstream ofs(dataTypeMapDb, ios::app);
    try {
        for (auto i = DataTypeMap.begin(); i != DataTypeMap.end(); i++) {
            ofs << i->first << "," << i->second << endl;
        }
    } catch(int e) {
        ofs.close();
        stringstream msg;
        msg << "failed to read " << dbName;
        throw kgError(msg.str());
    }
    ofs.close();
    
    // save data
    cerr << "writing " << this->dbName << " ..." << endl;
    if (clean) {
        FILE* fp = fopen(this->dbName.c_str(), "wb");
        fclose(fp);
    }
    
    FILE* fp = fopen(this->dbName.c_str(), "ab");
    if (fp == NULL) {
        stringstream msg;
        msg << "failed to open " + dbName;
        throw kgError(msg.str());
    }
    try {
        for (auto i = DataTypeMap.begin(); i != DataTypeMap.end(); i++) {
            kvHandle* kvh = NULL;
            pair<string, Ewah> ret;
            GetAllKeyValue(i->first, ret, kvh);
            while (kvh != NULL) {
//                cerr << "{" << i->first << "," << ret.first << "} ";
//                ret.second.printout(cerr);
                // key ('\0'まで書き込む）
                size_t rc = fwrite(i->first.c_str(), i->first.length() + 1, 1, fp);
                if (rc == 0) throw 0;
                
                // keyValue ('\0'まで書き込む）
                rc = fwrite(ret.first.c_str(), ret.first.length() + 1, 1, fp);
                if (rc == 0) throw 0;
                
                // bitmap with size
                stringstream ss;
                ret.second.write(ss);
                size_t size = ss.str().length();
                fwrite(&size, sizeof(size_t), 1, fp);
                if (rc == 0) throw 0;
                fwrite(ss.str().c_str(), size, 1, fp);
                if (rc == 0) throw 0;

                GetAllKeyValue(i->first, ret, kvh);
            }
        }
    } catch(int e) {
        fclose(fp);
        stringstream msg;
        msg << "failed to write " << this->dbName;
        throw kgError(msg.str());
    }
    fclose(fp);
}

void kgmod::BTree::load(void) {
    DataTypeMap.clear();
    str_btree.clear();
    num_btree.clear();
    
    // load dataTypeMap
    cerr << "reading " << dataTypeMapDb << " ..." << endl;
    string dt;
    ifstream ifs(dataTypeMapDb);
    try {
        while (getline(ifs, dt)) {
            vector<string> var;
            var = Cmn::CsvStr::Parse(dt);
            if (var.size() != 2) throw 0;
            DataType tmp = (DataType)stoi(var[1]);
            InitKey(var[0], tmp);
        }
    } catch (int e) {
        ifs.close();
        stringstream msg;
        msg << "failed to read " << this->dataTypeMapDb;
        throw kgError(msg.str());
    }
    ifs.close();
    
    // load data
    cerr << "reading " << this->dbName << " ..." << endl;
    size_t malloc_size = 1024;
    char* buf = (char*)malloc(malloc_size);
    FILE* fp = fopen(this->dbName.c_str(), "rb");
    if (fp == NULL) {
        stringstream msg;
        msg << "failed to open " + dbName;
        throw kgError(msg.str());
    }
    try {
        while (true) {
            // key
            string key = "";
            int c;
            int len = 0;
            while ((c = fgetc(fp)) != '\0') {
                if (c == EOF) break;
                key += (char)c;
                len++;
            }
            if (c == EOF && len == 0) break;
            if (c == EOF && len != 0) throw 0;

            string keyValue = "";
            while ((c = fgetc(fp)) != '\0') {
                if (c == EOF) throw 0;
                keyValue += (char)c;
            }
//            size_t ll = keyValue.length();
//            cerr << key << ":" << keyValue << " ";
            
            // bitmap
            size_t size;
            size_t rc = fread(&size, sizeof(size), 1, fp);
            if (rc == 0) throw 0;
            if (malloc_size < size) {malloc_size = size; buf = (char*)realloc(buf, malloc_size);};
            rc = fread(buf, size, 1, fp);
            if (rc == 0) {free(buf); throw 1;}
            
            stringstream ss;
            ss.write(buf, size);
            Ewah bmp;
            bmp.read(ss);
//            Cmn::CheckEwah(bmp);
            SetVal(key, keyValue, bmp);
        }
    } catch(int e) {
        free(buf);
        fclose(fp);
        stringstream msg;
        msg << "failed to read " << dbName;
        throw kgError(msg.str());
    }
    
    free(buf);
    fclose(fp);
}

void kgmod::BTree::dump(bool debug) {
    if (! debug) return;
    
    cerr << "dataTypeMapDB: " << dataTypeMapDb << endl;
    cerr << "DBname: " << dbName << endl;
    
    for (auto i = DataTypeMap.begin(); i != DataTypeMap.end(); i++) {
        cerr << "{" << i->first << "," << DataTypeStr[i->second] << "} ";
    }
    cerr << endl;
    
    for (auto i = DataTypeMap.begin(); i != DataTypeMap.end(); i++) {
        kvHandle* kvh = NULL;
        pair<string, Ewah> ret;
        GetAllKeyValue(i->first, ret, kvh);
        while (kvh != NULL) {
            cerr << "{" << i->first << "," << ret.first << "} ";
//            ret.second.printout(cerr);
            Cmn::CheckEwah(ret.second);
            GetAllKeyValue(i->first, ret, kvh);
        }
    }
}

vector<string> kgmod::BTree::EvalKeyValue(const string& Key, const Ewah* filter) {
    vector<string> out;
    if (DataTypeMap[Key] == STR) {
        for (auto i = str_btree.lower_bound({Key,""}); i != str_btree.end(); i++) {
            if (i->first.first != Key) break;
            if (filter == NULL) {
                out.push_back(i->first.second);
            } else {
                Ewah tmp;
                tmp = i->second & *filter;
                if (tmp.numberOfOnes() != 0) out.push_back(i->first.second);
            }
        }
    } else {
        for (auto i = num_btree.lower_bound({Key,-DBL_MAX}); i != num_btree.end(); i++) {
            if (i->first.first != Key) break;
            if (filter == NULL) {
                stringstream ss;
                ss << i->first.second;
                out.push_back(ss.str());
            } else {
                Ewah tmp;
                tmp = i->second & *filter;
                if (tmp.numberOfOnes() != 0) {
                    stringstream ss;
                    ss << i->first.second;
                    out.push_back(ss.str());
                }
            }
        }
    }
    return out;
}

size_t kgmod::BTree::CountKeyValue(const string& Key, const Ewah* filter) {
    size_t cnt = 0;
    if (DataTypeMap[Key] == STR) {
        for (auto i = str_btree.lower_bound({Key,""}); i != str_btree.end(); i++) {
            if (i->first.first != Key) break;
            if (filter == NULL) {
                cnt++;
            } else {
                Ewah zero;
                Ewah tmp = i->second & *filter;
                if (tmp != zero) cnt++;
            }
        }
    } else {
        for (auto i = num_btree.lower_bound({Key,-DBL_MAX}); i != num_btree.end(); i++) {
            if (i->first.first != Key) break;
            if (filter == NULL) {
                cnt++;
            } else {
                Ewah zero;
                Ewah tmp = i->second & *filter;
                if (tmp != zero) cnt++;
            }
        }
    }
    return cnt;
}
