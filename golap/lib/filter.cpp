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
#include "kgCSV.h"
#include "btree.hpp"
#include "bidx-ewah.hpp"
#include "cmn.hpp"
#include "filter.hpp"

using namespace std;
using namespace kgmod;

bool kgmod::Filter::existsFldName(const string& fldName, const tra_item traitem) {
    bool stat;
    if (traitem == TRA) {
        stat =  Cmn::posInVector(config->traAttFile.numFields, fldName) ||
                Cmn::posInVector(config->traAttFile.strFields, fldName);
//                Cmn::posInVector(config->traAttFile.catFields, fldName);
    } else if (traitem == ITEM) {
        stat =  Cmn::posInVector(config->itemAttFile.numFields, fldName) ||
                Cmn::posInVector(config->itemAttFile.strFields, fldName);
//                Cmn::posInVector(config->itemAttFile.catFields, fldName);
    } else {
        stringstream msg;
        msg << "invalid tra_item: " << traitem;
        throw kgError(msg.str());
    }
    return stat;
}

Ewah& kgmod::Filter::logicalnot(Ewah& bmp, const tra_item traitem) {
    size_t bmpCnt;
    if (traitem == TRA) {
        bmpCnt = occ->traAtt->traMax + 1;
    } else if (traitem == ITEM) {
        bmpCnt = occ->itemAtt->itemMax + 1;
    } else {
        stringstream msg;
        msg << "invalid tra_item: " << traitem;
        throw kgError(msg.str());
    }
    
    bmp.padWithZeroes(bmpCnt);
    bmp.inplace_logicalnot();
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
    if (! existsFldName(key, traitem)) throw kgError(key + " does not exist");
    Ewah bmp = isin(key, values, traitem);
    Ewah out = logicalnot(bmp, traitem);
    return out;
}

Ewah kgmod::Filter::like(const string& key, const values_t& values, const tra_item traitem) {
    if (! existsFldName(key, traitem)) throw kgError(key + " does not exist");
    BTree* bmpList = setModeTraItem(traitem);
    Ewah out;
    for (auto i = values.begin(); i != values.end(); i++) {
        Ewah tmp;
        bool stat = bmpList->GetValMulti(key, *i, tmp);
        if (! stat) throw kgError("error occures in search");
        out = out | tmp;
    }
    return out;
}

Ewah kgmod::Filter::search(const string& method, const string& key, const values_t& values, const tra_item traitem) {
    if (! existsFldName(key, traitem)) throw kgError(key + " does not exist");
    Ewah out;
    if (traitem != TRA) return out;
    for (auto i = values.begin(); i != values.end(); i++) {
        string kv;
        if (boost::iequals(method, "COMPLATE")) {
            kv = *i;
        } else if (boost::iequals(method, "SUBSTRING")) {
            kv = "*" + *i + "*";
        } else if (boost::iequals(method, "HEAD")) {
            kv = *i + "*";
        } else if (boost::iequals(method, "TAIL")) {
            kv = "*" + *i;
        } else {
            kv = *i;
        }
        
        Ewah tmp;
        bool stat = occ->bmpList.GetValMulti(key, kv, tmp);
        if (! stat) throw kgError("error occures in search");
        out = out | tmp;
    }
    return out;
}

Ewah kgmod::Filter::range(const string& key, const pair<string, string>& values, const tra_item traitem) {
    if (! existsFldName(key, traitem)) throw kgError(key + " does not exist");
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
Ewah kgmod::Filter::sel_item(string& itemFilter, const tra_item traitem) {
    if (traitem != TRA) throw 0;
    Ewah itemBmp = makeItemBitmap(itemFilter);
    Ewah out;
    for (auto i = itemBmp.begin(), ie = itemBmp.end(); i != ie; i++) {
        string tar = occ->itemAtt->item[*i];
        Ewah tmp = occ->bmpList.GetVal(occ->occKey, tar);
        out = out | tmp;
    }
    return out;
}

Ewah kgmod::Filter::del_item(string& itemFilter, const tra_item traitem) {
    if (traitem != TRA) throw 0;
    Ewah out = sel_item(itemFilter, traitem);
    out = logicalnot(out, traitem);
    return out;
}

Ewah kgmod::Filter::having(const string& key, string& andor, string& itemFilter, const tra_item traitem) {
    if (! existsFldName(key, traitem)) throw kgError(key + " does not exist");
    if (traitem != TRA) throw 0;
    Ewah itemBmp = makeItemBitmap(itemFilter);
    Ewah traBmp;
    if (itemBmp.numberOfOnes() == 0) return traBmp;
    
    if (boost::iequals(andor, "AND")) traBmp = logicalnot(traBmp, TRA);
    for (auto i = itemBmp.begin(); i != itemBmp.end(); i++) {
        Ewah tmp = occ->bmpList.GetVal(occ->occKey, occ->itemAtt->item[*i]);
        if (boost::iequals(andor, "AND")) {
            traBmp = traBmp & tmp;
        } else if (boost::iequals(andor, "OR")) {
            traBmp = traBmp | tmp;
        } else {
            throw 0;
        }
    }
    
    vector<string> values;
    btree::btree_map<string, bool> uniqCheck;
    BTree::kvHandle* kvh = NULL;
    pair<string, Ewah> ret;
    occ->bmpList.GetAllKeyValue(key, ret, kvh);
    while (kvh != NULL) {
        Ewah tmp = traBmp & ret.second;
        if (tmp.numberOfOnes() != 0) {
            if (uniqCheck.find(ret.first) == uniqCheck.end()) {
                values.push_back(ret.first);
                uniqCheck[ret.first] = true;
            }
        }
        occ->bmpList.GetAllKeyValue(key, ret, kvh);
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
    return func;
}

kgmod::Filter::values_t kgmod::Filter::getArg(char** cmdPtr) {
    go_next(cmdPtr);
    if (**cmdPtr != '(') throw 0;
    
    values_t arg;
    int argn = 0;
    char inQuote = '\0';
    bool isEscape = false;
    (*cmdPtr)++;    // 先頭は必ず'('なので、kakkoDepth = 1から始まる
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
                if (kakkoDepth < 0) throw 0;
                if (kakkoDepth == 0) continue;
            } else if (**cmdPtr == '"' || **cmdPtr == '\'') {
                if (kakkoDepth == 1) {
                    inQuote = **cmdPtr;
                    arg.push_back("");
                    continue;
                }
            } else if (**cmdPtr == ',') {
                if (kakkoDepth == 1) {argn++; continue;}
            } else if (isspace(**cmdPtr)) {
                continue;
            }
        }
        
        if (arg.size() == argn) arg.push_back("");
        char tmp[2];
        tmp[0] = **cmdPtr; tmp[1] = '\0';
        arg[argn] += tmp;
        
        isEscape = false;
    }
    
    if (arg.size() == argn) arg.push_back("");
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
            if (boost::iequals(func, "NOT")) {
                if (arg.size() != 1) throw 0;
                char* cmd = (char*)arg[0].c_str();
                Ewah tmp = runcmd(&cmd, traitem);
                out = logicalnot(tmp, traitem);
            } else if (boost::iequals(func, "AND")) {
                if (arg.size() < 2) throw 0;
                char* cmd = (char*)arg[0].c_str();
                out  = runcmd(&cmd, traitem);
                for (int i = 1; i < arg.size(); i++) {
                    cmd = (char*)arg[i].c_str();
                    Ewah tmp = runcmd(&cmd, traitem);
                    out = out & tmp;
                }
            } else if (boost::iequals(func, "OR")) {
                if (arg.size() < 2) throw 0;
                char* cmd = (char*)arg[0].c_str();
                out  = runcmd(&cmd, traitem);
                for (int i = 1; i < arg.size(); i++) {
                    cmd = (char*)arg[i].c_str();
                    Ewah tmp = runcmd(&cmd, traitem);
                    out = out | tmp;
                }
            } else if (boost::iequals(func, "MINUS")) {
                if (arg.size() < 2) throw 0;
                char* cmd = (char*)arg[0].c_str();
                out  = runcmd(&cmd, traitem);
                for (int i = 1; i < arg.size(); i++) {
                    cmd = (char*)arg[i].c_str();
                    Ewah tmp = runcmd(&cmd, traitem);
                    out = out - tmp;
                }
            } else if (boost::iequals(func, "SEL")) {
                out = sel(arg, traitem);
            } else if (boost::iequals(func, "DEL")) {
                out = del(arg, traitem);
            } else if (boost::iequals(func, "ISIN")) {
                string key = arg[0];
                arg.erase(arg.begin());
                out = isin(key, arg, traitem);
            } else if (boost::iequals(func, "ISNOTIN")) {
                string key = arg[0];
                arg.erase(arg.begin());
                out = isnotin(key, arg, traitem);
            } else if (boost::iequals(func, "LIKE")) {
                string key    = arg[0]; arg.erase(arg.begin());
                out = like(key, arg, traitem);
            } else if (boost::iequals(func, "SEARCH")) {
                string method = arg[0]; arg.erase(arg.begin());
                string key    = arg[0]; arg.erase(arg.begin());
                out = search(method, key, arg, traitem);
            } else if (boost::iequals(func, "RANGE")) {
                if (arg.size() <= 1) {
                    arg.resize(3);
                } else if (arg.size() == 2) {
                    arg.resize(3);
                    arg[2] = arg[1];
                    size_t l = arg[2].size();
                    if (l != 0) arg[2][l - 1]++;
                }
                string key = arg[0]; arg.erase(arg.begin());
                string st  = arg[0]; arg.erase(arg.begin());
                string ed  = arg[0]; arg.erase(arg.begin());
                pair<string, string> fromto(st, ed);
                out = range(key, fromto, traitem);
            } else if (boost::iequals(func, "SEL_ITEM")) {
                out = sel_item(arg[0], traitem);
            } else if (boost::iequals(func, "DEL_ITEM")) {
                out = del_item(arg[0], traitem);
            } else if (boost::iequals(func, "HAVING")) {
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
    catch(kgError& err){
        auto msg = err.message();
        stringstream buf;
        for (auto& m : msg) buf << m << "\n";
        buf << "error occerred in " << rcmd;
        throw kgError(buf.str());
    }
}

Ewah kgmod::Filter::makeTraBitmap(string& cmdline) {
    boost::trim(cmdline);
    Ewah bmp;
    if (cache->get(cmdline, {}, TRA, bmp)) {
        cerr << "(tra) executing " << cmdline << endl;
        cerr << "found in cache" << endl;
    } else {
        char* cmd = (char*)cmdline.c_str();
        if (cmdline == "") {
            cerr << "(tra) all transactions" << endl;
            bmp = allone(TRA);
        } else {
            cerr << "(tra) executing " << cmdline << endl;
            bmp = runcmd(&cmd, TRA);
            cache->put(cmdline, {}, TRA, bmp);
        }
    }
    cerr << "(tra) result: "; Cmn::CheckEwah(bmp);
    return bmp;
}

Ewah kgmod::Filter::makeItemBitmap(string& cmdline) {
    boost::trim(cmdline);
    Ewah bmp;
    if (cache->get(cmdline, {}, ITEM, bmp)) {
        cerr << "(item) executing " << cmdline << endl;
        cerr << "found in cache" << endl;
    } else {
        char* cmd = (char*)cmdline.c_str();
        if (cmdline == "") {
            cerr << "(item) all items" << endl;
            bmp = allone(ITEM);
        } else {
            cerr << "(item) executing " << cmdline << endl;
            bmp = runcmd(&cmd, ITEM);
            cache->put(cmdline, {}, ITEM, bmp);
        }
        cerr << "(item) result: "; Cmn::CheckEwah(bmp);
    }
    return bmp;
}
