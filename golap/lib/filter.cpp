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

kgmod::cmdCache::cmdCache(Config* config, const bool init)
: lru_cache(config->cmdCache_size), config(config) , modCount(0) {
    fileName = config->dbDir + "/cmdCache.dat";
    if (init) {
        initFile();
    } else {
        load();
    }
};

string kgmod::cmdCache::makeKey(const string& func, const vector<string>& arg, const tra_item traitem) {
    stringstream ss;
    ss << traitem;
    string tmp;
    for (auto i = arg.begin(); i != arg.end(); i++) {
        tmp += *i;
    }
    return ss.str() + func + tmp;
}

void kgmod::cmdCache::put(const string& key, const Ewah& bmp, const bool reverse) {
    if (! config->cmdCache_enable) return;
    if (reverse) {
        cmd_cache_t::put_r(key, bmp);
    } else {
        cmd_cache_t::put(key, bmp);
    }
    checkPoint();
}

void kgmod::cmdCache::put(const string& func, const vector<string>& arg, const tra_item traitem,
                          const Ewah& bmp, const bool reverse) {
    if (! config->cmdCache_enable) return;
    string key = makeKey(func, arg, traitem);
    put(key, bmp, reverse);
}

bool kgmod::cmdCache::get(const string& key, Ewah& bmp) {
    if (! config->cmdCache_enable) return false;
    if (! cmd_cache_t::exists(key)) return false;
    bmp = cmd_cache_t::get(key);
    return true;
}

bool kgmod::cmdCache::get(const string& func, const vector<string>& arg, const tra_item traitem, Ewah& bmp) {
    if (! config->cmdCache_enable) return false;
    string key = makeKey(func, arg, traitem);
    return get(key, bmp);
}

bool kgmod::cmdCache::exists(const string& key) {
    if (! config->cmdCache_enable) return false;
    return cmd_cache_t::exists(key);
}

bool kgmod::cmdCache::exists(const string& func, const vector<string>& arg, const tra_item traitem) {
    if (! config->cmdCache_enable) return false;
    string key = makeKey(func, arg, traitem);
    return exists(key);
}

void kgmod::cmdCache::initFile(void) {
    FILE* fp = fopen(fileName.c_str(), "w");
    if (fp == NULL) {
        stringstream msg;
        msg << "failed to open " + fileName;
        throw kgError(msg.str());
    }
    fclose(fp);
}

void kgmod::cmdCache::save(void) {
    if (! config->cmdCache_enable) return;
    if (modCount == 0) return;

    FILE* fp = fopen(fileName.c_str(), "wb");
    if (fp == NULL) {
        stringstream msg;
        msg << "failed to open " + fileName;
        throw kgError(msg.str());
    }

    try {
        for (auto i = _cache_items_list.cbegin(); i != _cache_items_list.cend(); i++) {
            size_t rc;
            char* cmd = (char*)i->first.c_str();
            size_t size = i->first.size();
            rc = fwrite(&size, sizeof(size_t), 1, fp);
            if (rc == 0) throw 0;
            rc = fwrite(cmd, size, 1, fp);
            if (rc == 0) throw 0;

            stringstream ss;
            i->second.write(ss);
            size_t sssize = ss.str().size();
            rc = fwrite(&sssize, sizeof(size_t), 1, fp);
            if (rc == 0) throw 0;
            rc = fwrite(ss.str().c_str(), sssize, 1, fp);
            if (rc == 0) throw 0;
        }
    } catch(int e) {
        fclose(fp);
        stringstream msg;
        msg << "failed to write " << fileName;
        throw kgError(msg.str());
    }

    fclose(fp);
}

void kgmod::cmdCache::load(void) {
    if (! config->cmdCache_enable) return;

    FILE* fp = fopen(fileName.c_str(), "rb");
    if (fp == NULL) {
        stringstream msg;
        msg << "failed to open " + fileName;
        throw kgError(msg.str());
    }

    try {
        while (true) {
            size_t rc;
            size_t keyLen;
            rc = fread(&keyLen, sizeof(keyLen), 1, fp);
            if (rc == 0) break;
            char* keyBuf = (char*)malloc(keyLen + 1);
            rc = fread(keyBuf, keyLen, 1, fp);
            keyBuf[keyLen] = '\0';
            if (rc == 0) {free(keyBuf); throw 1;}
            string key(keyBuf);

            size_t ssSize;
            rc = fread(&ssSize, sizeof(ssSize), 1, fp);
            if (rc == 0) {free(keyBuf); throw 1;}
            char* ssBuf = (char*)malloc(ssSize);
            rc = fread(ssBuf, ssSize, 1, fp);
            if (rc == 0) {free(keyBuf); free(ssBuf); throw 1;}

            stringstream ss;
            ss.write(ssBuf, ssSize);
            Ewah bmp;
            bmp.read(ss);
            put(key, bmp, true);

            free(keyBuf); free(ssBuf);
        }
    } catch(int e) {
        fclose(fp);
        stringstream msg;
        msg << "failed to read " << fileName;
        throw kgError(msg.str());
    }

    fclose(fp);
}

void kgmod::cmdCache::clear(void) {
    _cache_items_list.clear();
    _cache_items_map.clear();
}

void kgmod::cmdCache::dump(bool debug) {
    if (! config->cmdCache_enable) return;
    if (! debug) return;

    cerr << "<<< dump command cache >>>" << endl;
    for (auto i = _cache_items_list.cbegin(); i != _cache_items_list.cend(); i++) {
        cerr << i->first << "-> ";
        Ewah bmp = i->second;
//        bmp.printout(cerr);
        Cmn::CheckEwah(bmp);
    }
    cerr << endl;
}

void kgmod::cmdCache::checkPoint(void) {
    modCount++;
    if (modCount > config->cmdCache_saveInterval) {
        save();
        modCount = 0;
    }
}



bool kgmod::Filter::existsFldName(const string& fldName, const tra_item traitem) {
    bool stat;
    if (traitem == TRA) {
        stat =  Cmn::posInVector(config->traAttFile.numFields, fldName) ||
                Cmn::posInVector(config->traAttFile.strFields, fldName);
    } else if (traitem == ITEM) {
        stat =  Cmn::posInVector(config->itemAttFile.numFields, fldName) ||
                Cmn::posInVector(config->itemAttFile.strFields, fldName);
    } else if (traitem == FACT) {
        stat =  Cmn::posInVector(config->traFile.numFields, fldName) ||
                Cmn::posInVector(config->traFile.strFields, fldName);
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
        bmpCnt = occ->traMax() + 1;
    } else if (traitem == ITEM) {
        bmpCnt = occ->itemMax() + 1;
    } else if (traitem == FACT) {
        bmpCnt = fact->recMax + 1;
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
        bmpList = occ->getTraBtree();
    } else if (traitem == ITEM) {
        bmpList = occ->getItemBtree();
    } else if (traitem == FACT) {
        bmpList = &(fact->bmplist);
    } else {
        stringstream msg;
        msg << "invalid tra_item: " << traitem;
        throw kgError(msg.str());
    }
    return bmpList;
}
/*
pair<BTree*, string> kgmod::Filter::setModeTraItem2(const tra_item traitem) {
    BTree* bmpList;
    string key;
    if (traitem == TRA) {
      	bmpList = occ->getTraBtree();
       key = config->traFile.traFld;
    } else if (traitem == ITEM) {
      	bmpList = occ->getItemBtree();
        key = config->traFile.itemFld;
    } else if (traitem == FACT) {
        bmpList = &(fact->bmplist);
        key = "";
    } else {
        stringstream msg;
        msg << "invalid tra_item: " << traitem;
        throw kgError(msg.str());
    }

    pair<BTree*, string> out = {bmpList, key};
    return out;
}
*/
Ewah kgmod::Filter::sel(const values_t& values, const tra_item traitem) {
    Ewah out;
    for (auto val = values.begin(); val != values.end(); val++) {
        size_t num;
        if (traitem == TRA) {
            boost::optional<size_t> tmp = occ->getTraID(*val);
            num = *tmp;
        } else if (traitem == ITEM) {
            boost::optional<size_t> tmp = occ->getItemID(*val);
            num = *tmp;
        } else if (traitem == FACT) {
            stringstream msg;
            msg << "cannot use sel for fact filter" << traitem;
            throw kgError(msg.str());
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
    if (traitem == FACT) {
        stringstream msg;
        msg << "cannot use del for fact filter" << traitem;
        throw kgError(msg.str());
    }

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
        bool stat = occ->getTraBmpM(key, kv, tmp);
        if (! stat) throw kgError("error occures in search");
        out = out | tmp;
    }
    return out;
}

Ewah kgmod::Filter::range(const string& key, const pair<string, string>& values, const tra_item traitem) {
    if (! existsFldName(key, traitem)) throw kgError(key + " does not exist");
    Ewah out;
    BTree* bmpList;
    if (traitem == TRA) {
        bmpList = occ->getTraBtree();
    } else if (traitem == ITEM) {
        bmpList = occ->getItemBtree();
    } else if (traitem == FACT) {
        bmpList = &(fact->bmplist);
    } else {
        stringstream msg;
        msg << "invalid tra_item: " << traitem;
        throw kgError(msg.str());
    }

    bool stat = bmpList->GetValMulti(key, "[", values.first, "]", values.second, out);
    if (! stat) throw kgError("error occures in search");
    return out;
}

// trafilter専用
Ewah kgmod::Filter::sel_item(string& itemFilter, const tra_item traitem) {
    if (traitem != TRA) throw 0;
    Ewah itemBmp = makeItemBitmap(itemFilter);
    Ewah out;
    for (auto i = itemBmp.begin(), ie = itemBmp.end(); i != ie; i++) {
        //string tar = occ->itemAtt->item[*i];
        //Ewah tmp = occ->bmpList.GetVal(occ->occKey, tar);
        // out = out | tmp;
        out = out | occ->getTraBmpFromItem(*i);
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

        //Ewah tmp = occ->bmpList.GetVal(occ->occKey, occ->itemAtt->item[*i]);

        Ewah tmp = occ->getTraBmpFromItem(*i);


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
    occ->GetAllKeyValue(key, ret, kvh);
    while (kvh != NULL) {
        Ewah tmp = traBmp & ret.second;
        if (tmp.numberOfOnes() != 0) {
            if (uniqCheck.find(ret.first) == uniqCheck.end()) {
                values.push_back(ret.first);
                uniqCheck[ret.first] = true;
            }
        }
        occ->GetAllKeyValue(key, ret, kvh);
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
    vector<bool> isQuoted = {false};
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
		                isQuoted[argn] = true;
                    inQuote = **cmdPtr;
                    arg.push_back("");
                    continue;
                }
            } else if (**cmdPtr == ',') {
                if (kakkoDepth == 1) {
                	argn++;
                	isQuoted.resize(argn + 1); isQuoted[argn] = false;
                	continue;
                }
            }
            else if (isspace(**cmdPtr)) {
              if (arg.size() == argn) continue;
              if (isQuoted[argn]) continue;
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
    for (size_t i = 0; i < arg.size(); i++) {
        if (!isQuoted[i]) Cmn::chomp(arg[i]);
    }
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
Ewah kgmod::Filter::makeFactBitmap(string& cmdline) {
    boost::trim(cmdline);
    Ewah bmp;
    if (cache->get(cmdline, {}, FACT, bmp)) {
        cerr << "(item) executing " << cmdline << endl;
        cerr << "found in cache" << endl;
    } else {
        char* cmd = (char*)cmdline.c_str();
        if (cmdline == "") {
            cerr << "(item) all items" << endl;
            bmp = allone(ITEM);
        } else {
            cerr << "(item) executing " << cmdline << endl;
            bmp = runcmd(&cmd, FACT);
            cache->put(cmdline, {}, FACT, bmp);
        }
        cerr << "(item) result: "; Cmn::CheckEwah(bmp);
    }
    return bmp;
}

// TraID一覧をファイルで受けて、Filter Bitmapとする
Ewah kgmod::Filter::makeTraFileFilter(string& infile) {
    Ewah out;
    string fileName;
    fileName = config->inDir + "/" + infile;
    if (infile != "" && Cmn::FileExists(fileName)) {
        set<size_t> bitlist;
        size_t tra_colNo = -1;
        bool first = true;
        string line;
        ifstream ifs(fileName);
        while (getline(ifs, line)) {
            vector<string> buf = Cmn::CsvStr::Parse(line);
            if (first) {
                string reg;
                reg = config->traFile.traFld + "(|%[0-9]+|%[0-9]+r|%[0-9]+n|%[0-9]+nr)";
                tra_colNo = Cmn::find_regex(buf, reg);
                if (tra_colNo == -1) break;
                first = false;
            } else {
                boost::optional<size_t> traNo = occ->getTraID(buf[tra_colNo]);
                if (traNo) bitlist.insert(*traNo);
            }
        }

        if (tra_colNo == -1) {
            out.padWithZeroes(occ->traMax() + 1);
            out.inplace_logicalnot();
        } else {
            for (size_t t : bitlist) {
                out.set(t);
            }
        }
    } else {
        out.padWithZeroes(occ->traMax() + 1);
        out.inplace_logicalnot();
    }
    return out;
}

Ewah kgmod::Filter::makeItemFileFilter(string& infile) {
    Ewah out;
    string fileName;
    fileName = config->inDir + "/" + infile;
    if (infile != "" && Cmn::FileExists(fileName)) {
        set<size_t> bitlist;
        size_t item_colNo = -1;
        bool first = true;
        string line;
        ifstream ifs(fileName);
        while (getline(ifs, line)) {
            vector<string> buf = Cmn::CsvStr::Parse(line);
            if (first) {
                string reg;
                reg = config->traFile.itemFld + "(|%[0-9]+|%[0-9]+r|%[0-9]+n|%[0-9]+nr)";
                item_colNo = Cmn::find_regex(buf, reg);
                if (item_colNo == -1) break;
                first = false;
            } else {
                boost::optional<size_t> traNo = occ->getItemID(buf[item_colNo]);
                if (traNo) bitlist.insert(*traNo);
            }
        }

        if (item_colNo == -1) {
            out.padWithZeroes(occ->itemMax() + 1);
            out.inplace_logicalnot();
        } else {
            for (size_t t : bitlist) {
                out.set(t);
            }
        }
    } else {
        out.padWithZeroes(occ->itemMax() + 1);
        out.inplace_logicalnot();
    }
    return out;
}

Ewah kgmod::Filter::makeFactFileFilter(string& infile) {
    Ewah out;
    string fileName;
    fileName = config->inDir + "/" + infile;
    if (infile != "" && Cmn::FileExists(fileName)) {
        set<size_t> bitlist;
        size_t tra_colNo = -1;
        size_t item_colNo = -1;
        size_t cnt = 0;
        string line;
        ifstream ifs(fileName);
        while (getline(ifs, line)) {
            vector<string> buf = Cmn::CsvStr::Parse(line);
            if (cnt == 0) {
                string reg;
                reg = config->traFile.traFld + "(|%[0-9]+|%[0-9]+r|%[0-9]+n|%[0-9]+nr)";
                tra_colNo = Cmn::find_regex(buf, reg);
                if (tra_colNo == -1) break;

                reg = config->traFile.itemFld + "(|%[0-9]+|%[0-9]+r|%[0-9]+n|%[0-9]+nr)";
                item_colNo = Cmn::find_regex(buf, reg);
                if (item_colNo == -1) break;
            } else {
                boost::optional<size_t> traNo = occ->getTraID(buf[tra_colNo]);
                boost::optional<size_t> itemNo = occ->getItemID(buf[item_colNo]);
                boost::optional<size_t> recNo = fact->keys2recNo(*traNo, *itemNo);
                if (recNo) bitlist.insert(*recNo);
            }
            cnt++;
        }

        if (cnt == 0) {
            out.padWithZeroes(fact->recMax + 1);
            out.inplace_logicalnot();
        } else {
            for (size_t t : bitlist) {
                out.set(t);
            }
        }
    } else {
        out.padWithZeroes(fact->recMax + 1);
        out.inplace_logicalnot();
    }
    return out;
}
