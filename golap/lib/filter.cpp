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

#include <string>
#include <algorithm>
#include <vector>
#include <kgError.h>
#include <kgMethod.h>
#include <kgConfig.h>
#include <kgMessage.h>
#include <boost/algorithm/string.hpp>
#include "kgCsv.h"
#include "btree.hpp"
#include "bidx-ewah.hpp"
#include "cmn.hpp"
#include "filter.hpp"

using namespace std;
using namespace kgmod;

Ewah& kgmod::Filter::logicalnot(Ewah& bmp, const tra_item traitem) {
    size_t bmpMax;
    if (traitem == TRA) {
        bmpMax = occ->traAtt->traMax;
    } else if (traitem == ITEM) {
        bmpMax = occ->itemAtt->itemMax;
    } else {
        stringstream msg;
        msg << "invalid tra_item: " << traitem;
        throw kgError(msg.str());
    }
    
    bmp.padWithZeroes(bmpMax + 1);
    bmp = bmp.logicalnot();
    return bmp;
}

Ewah kgmod::Filter::allone(const tra_item traitem) {
    Ewah out;
    return logicalnot(out, traitem);
}

BTree* kgmod::Filter::setModeTraItem(const tra_item traitem) {
    BTree* bmpList;
    if (traitem == TRA) {
        bmpList = &(occ->bmpList);
    } else if (traitem == ITEM) {
        bmpList = &(occ->itemAtt->bmpList);
    } else {
        stringstream msg;
        msg << "invalid tra_item: " << traitem;
        throw kgError(msg.str());
    }
    return bmpList;
}

pair<BTree*, string> kgmod::Filter::setModeTraItem2(const tra_item traitem) {
    BTree* bmpList;
    string key;
    if (traitem == TRA) {
        bmpList = &(occ->bmpList);
        key = config->traFile.traFld;
    } else if (traitem == ITEM) {
        bmpList = &(occ->itemAtt->bmpList);
        key = config->traFile.itemFld;
    } else {
        stringstream msg;
        msg << "invalid tra_item: " << traitem;
        throw kgError(msg.str());
    }
    
    pair<BTree*, string> out = {bmpList, key};
    return out;
}

Ewah kgmod::Filter::sel(const values_t& values, const tra_item traitem) {
    Ewah out;
    for (auto val = values.begin(); val != values.end(); val++) {
        size_t num;
        if (traitem == TRA) {
            num = occ->traAtt->traNo[*val];
        } else if (traitem == ITEM) {
            num = occ->itemAtt->itemNo[*val];
        } else {
            stringstream msg;
            msg << "invalid tra_item: " << traitem;
            throw kgError(msg.str());
        }
        
        Ewah bmp;
        bmp.set(num);
        out = out | bmp;
    }
    return out;
}

Ewah kgmod::Filter::del(const values_t& values, const tra_item traitem) {
    Ewah bmp = sel(values, traitem);
    Ewah out = logicalnot(bmp, traitem);
    return out;
}

Ewah kgmod::Filter::isin(const string& key, const values_t& values, const tra_item traitem) {
    BTree* bmpList = setModeTraItem(traitem);
    Ewah out;
    for (auto val = values.begin(); val != values.end(); val++) {
        Ewah bmp = bmpList->GetVal(key, *val);
        out = out | bmp;
    }
    return out;
}

Ewah kgmod::Filter::isnotin(const string& key, const values_t& values, const tra_item traitem) {
    Ewah bmp = isin(key, values, traitem);
    Ewah out = logicalnot(bmp, traitem);
    return out;
}

Ewah kgmod::Filter::search(string& method, const string& key, const values_t& values, const tra_item traitem) {
    Ewah out;
    BTree* bmpList;
    
    transform(method.cbegin(), method.cend(), method.begin(), ::toupper);
    for (auto i = values.begin(); i != values.end(); i++) {
        string kv;
        if (method == "COMPLATE") {
            kv = *i;
        } else if (method == "SUBSTRING") {
            kv = "*" + *i + "*";
        } else if (method == "HEAD") {
            kv = *i + "*";
        } else if (method == "TAIL") {
            kv = "*" + *i;
        }
        
        string k;
        if (traitem == TRA) {
            bmpList = &(occ->bmpList);
            k = config->traFile.traFld;
        } else if (traitem == ITEM) {
            bmpList = &(occ->itemAtt->bmpList);
            k = config->traFile.itemFld;
        } else {
            stringstream msg;
            msg << "invalid tra_item: " << traitem;
            throw kgError(msg.str());
        }
        
        Ewah tmp;
        bool stat = bmpList->GetValMulti(k, kv, tmp);
        if (! stat) throw kgError("error occures in search");
        out = out & tmp;
    }
    return out;
}

Ewah kgmod::Filter::range(const string& key, const pair<string, string>& values, const tra_item traitem) {
    Ewah out;
    BTree* bmpList;
    
    string k;
    if (traitem == TRA) {
        bmpList = &(occ->bmpList);
        k = config->traFile.traFld;
    } else if (traitem == ITEM) {
        bmpList = &(occ->itemAtt->bmpList);
        k = config->traFile.itemFld;
    } else {
        stringstream msg;
        msg << "invalid tra_item: " << traitem;
        throw kgError(msg.str());
    }
    
    bool stat = bmpList->GetValMulti(key, "[", values.first, "]", values.second, out);
//    out.printout();
    if (! stat) throw kgError("error occures in search");
    return out;
}

// trafilter専用
Ewah kgmod::Filter::sel_item(const string& key, const values_t& values, const tra_item traitem) {
    if (traitem != TRA) throw 0;
    Ewah out;
    for (auto i = values.begin(); i != values.end(); i++) {
        Ewah tmp = occ->bmpList.GetVal(key, *i);
        out = out | tmp;
    }
    return out;
}

Ewah kgmod::Filter::del_item(const string& key, const values_t& values, const tra_item traitem) {
    if (traitem != TRA) throw 0;
    Ewah out = sel_item(key, values, traitem);
    out = logicalnot(out, traitem);
    return out;
}

Ewah kgmod::Filter::having(const string& key, string& andor, string& itemFilter, const tra_item traitem) {
    if (traitem != TRA) throw 0;
    Ewah itemBmp = makeItemBitmap(itemFilter);
    Ewah traBmp;
    transform(andor.cbegin(), andor.cend(), andor.begin(), ::toupper);
    if (andor == "AND") traBmp = logicalnot(traBmp, TRA);
    for (auto i = itemBmp.begin(); i != itemBmp.end(); i++) {
        Ewah tmp = occ->bmpList.GetVal(occ->occKey, occ->itemAtt->item[*i]);
        if (andor == "AND") {
            traBmp = traBmp & tmp;
        } else if (andor == "OR") {
            traBmp = traBmp | tmp;
        } else {
            throw 0;
        }
    }
    
    vector<string> values;
    btree::btree_map<string, bool> uniqCheck;
    pair<string, Ewah> ret;
    size_t cur = 0;
    ret = occ->bmpList.GetAllKeyValue(key, cur);
    while (cur != -1) {
        Ewah tmp = traBmp & ret.second;
        if (tmp.numberOfOnes() != 0) {
            if (uniqCheck.find(ret.first) == uniqCheck.end()) {
                values.push_back(ret.first);
                uniqCheck[ret.first] = true;
            }
        }
        ret = occ->bmpList.GetAllKeyValue(key, cur);
    }
    
    if (debug) {
        int c = 0;
        cerr << "(internal) ISIN(tra): ";
        cerr << key << " ";
        cerr << "(" << values.size() << ")";
        for (auto i = values.begin(); i != values.end(); i++) {
            cerr << " " << *i;
            if (c > 10) break;
            c++;
        }
        if (c > 10) cerr << " ...";
        cerr << endl;
    }

    Ewah out = isin(key, values, TRA);
    return out;
}

// コマンドラインの解析
void kgmod::Filter::go_next(char** cmdPtr) {
    while (isspace(**cmdPtr)) {
        (*cmdPtr)++;
    }
}

string kgmod::Filter::getFunc(char** cmdPtr) {
    go_next(cmdPtr);
    string func;
    while (isalpha(**cmdPtr) || **cmdPtr =='_') {
        func += **cmdPtr;
        (*cmdPtr)++;
    }
    transform(func.cbegin(), func.cend(), func.begin(), ::toupper);
    return func;
}

kgmod::Filter::values_t kgmod::Filter::getArg(char** cmdPtr) {
    go_next(cmdPtr);
    if (**cmdPtr != '(') throw 0;
    
    values_t arg;
    int argn = 0;
    char inQuote = '\0';
    bool isEscape = false;
    (*cmdPtr)++;
    for (int kakkoDepth = 1; kakkoDepth > 0; (*cmdPtr)++) {
        if (inQuote) {
            if (! isEscape) {
                if (**cmdPtr == '\\') {
                    isEscape = true;
                    continue;
                } else if (**cmdPtr == inQuote) {
                    inQuote = '\0';
                    continue;
                }
            }
        } else {
//            if (! isprint(**cmdPtr) || **cmdPtr == '\0') throw 0;
            if (**cmdPtr == '\0') throw 0;
            if (**cmdPtr == '(') {
                kakkoDepth++;
                if (kakkoDepth == 1) continue;
            } else if (**cmdPtr == ')') {
                kakkoDepth--;
                if (kakkoDepth == 0) continue;
                if (kakkoDepth < 0) throw 0;
            } else if (**cmdPtr == '"' || **cmdPtr == '\'') {
                if (kakkoDepth == 1) {
                    inQuote = **cmdPtr;
                    continue;
                }
            } else if (**cmdPtr == ',') {
                if (kakkoDepth == 1) {
                    argn++;
                    continue;
                }
            } else if (isspace(**cmdPtr)) {
                continue;
            }
        }
        
        if (arg.size() == argn) {
            arg.push_back("");
        }
        
        char tmp[2];
        tmp[0] = **cmdPtr; tmp[1] = '\0';
        arg[argn] += tmp;
        
        isEscape = false;
    }
    
    if (*(*cmdPtr - 1) != ')') throw 0;
    return arg;
}

Ewah kgmod::Filter::runcmd(char** cmdPtr, const tra_item traitem) {
    char* rcmd = *cmdPtr;
    try {
        Ewah out;
        go_next(cmdPtr);
        if (isprint(**cmdPtr)) {
            string func = getFunc(cmdPtr);
            if (debug) cerr << func << ": ";
            vector<string> arg = getArg(cmdPtr);
            if (debug) {for (auto i = arg.begin(); i != arg.end(); i++) cerr << *i << " "; cerr << endl;}
            if (func == "NOT") {
                if (arg.size() != 1) throw 0;
                char* cmd = (char*)arg[0].c_str();
                Ewah tmp = runcmd(&cmd, traitem);
                out = logicalnot(tmp, traitem);
            } else if (func == "AND") {
                if (arg.size() < 2) throw 0;
                char* cmd = (char*)arg[0].c_str();
                out  = runcmd(&cmd, traitem);
                for (int i = 1; i < arg.size(); i++) {
                    cmd = (char*)arg[i].c_str();
                    Ewah tmp = runcmd(&cmd, traitem);
                    out = out & tmp;
                }
            } else if (func == "OR") {
                if (arg.size() < 2) throw 0;
                char* cmd = (char*)arg[0].c_str();
                out  = runcmd(&cmd, traitem);
                for (int i = 1; i < arg.size(); i++) {
                    cmd = (char*)arg[i].c_str();
                    Ewah tmp = runcmd(&cmd, traitem);
                    out = out | tmp;
                }
            } else if (func == "MINUS") {
                if (arg.size() < 2) throw 0;
                char* cmd = (char*)arg[0].c_str();
                out  = runcmd(&cmd, traitem);
                for (int i = 1; i < arg.size(); i++) {
                    cmd = (char*)arg[i].c_str();
                    Ewah tmp = runcmd(&cmd, traitem);
                    out = out - tmp;
                }
            } else if (func == "SEL") {
                out = sel(arg, traitem);
            } else if (func == "DEL") {
                out = del(arg, traitem);
            } else if (func == "ISIN") {
                string key = arg[0];
                arg.erase(arg.begin());
                out = isin(key, arg, traitem);
            } else if (func == "ISNOTIN") {
                string key = arg[0];
                arg.erase(arg.begin());
                out = isnotin(key, arg, traitem);
            } else if (func == "SEARCH") {
                string method = arg[0]; arg.erase(arg.begin());
                string key    = arg[0]; arg.erase(arg.begin());
                out = search(method, key, arg, traitem);
            } else if (func == "RANGE") {
                string key = arg[0]; arg.erase(arg.begin());
                string st  = arg[0]; arg.erase(arg.begin());
                string ed  = arg[0]; arg.erase(arg.begin());
                pair<string, string> fromto(st, ed);
                out = range(key, fromto, traitem);
            } else if (func == "SEL_ITEM") {
                string key = arg[0]; arg.erase(arg.begin());
                out = sel_item(key, arg, traitem);
            } else if (func == "DEL_ITEM") {
                string key = arg[0]; arg.erase(arg.begin());
                out = del_item(key, arg, traitem);
            } else if (func == "HAVING") {
                bool stat = cache->get(func, arg, traitem, out);
                if (! stat) {
                    if (arg.size() != 3) throw 0;
                    out = having(arg[0], arg[1], arg[2], traitem);
                    cache->put(func, arg, traitem, out);
                }
            } else {
                throw 0;
            }
        }
        return out;
    }
    catch (int e) {
        stringstream msg;
        msg << "syntax error: " << rcmd;
        throw kgError(msg.str());
    }
}

Ewah kgmod::Filter::makeTraBitmap(string& cmdline) {
    boost::trim(cmdline);
    Ewah bmp;
    if (cache->get(cmdline, {}, TRA, bmp)) {
        cerr << "(tra) executing " << cmdline << endl;
        cerr << "found in cache" << endl;
        cerr << "(tra) result: "; Cmn::CheckEwah(bmp);
    } else {
        char* cmd = (char*)cmdline.c_str();
        if (cmdline == "") {
            bmp = allone(TRA);
        } else {
            cerr << "(tra) executing " << cmdline << endl;
            bmp = runcmd(&cmd, TRA);
            cache->put(cmdline, {}, TRA, bmp);
            cerr << "(tra) result: "; Cmn::CheckEwah(bmp);
        }
    }
    return bmp;
}

Ewah kgmod::Filter::makeItemBitmap(string& cmdline) {
    boost::trim(cmdline);
    Ewah bmp;
    if (cache->get(cmdline, {}, ITEM, bmp)) {
        cerr << "(item) executing " << cmdline << endl;
        cerr << "found in cache" << endl;
        cerr << "(item) result: "; Cmn::CheckEwah(bmp);
    } else {
        char* cmd = (char*)cmdline.c_str();
        if (cmdline == "") {
            bmp = allone(ITEM);
        } else {
            cerr << "(item) executing " << cmdline << endl;
            bmp = runcmd(&cmd, ITEM);
            cache->put(cmdline, {}, ITEM, bmp);
            cerr << "(item) result: "; Cmn::CheckEwah(bmp);
        }
    }
    return bmp;
}
