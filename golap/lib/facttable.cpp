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
#include <cstdint>
#include <stdint.h>
#include <map>
#include <boost/algorithm/string.hpp>
#include <kgCSV.h>
#include <kgError.h>
#include "cmn.hpp"
#include "facttable.hpp"

using namespace kgmod;
using namespace std;

// class AargFunc
string kgmod::AggrFunc::getOneElem(char** ptr) {
    string out;
    while (isspace(**ptr)) (*ptr)++;
    if (strchr("+-*/(),", **ptr) != NULL) {
        out.push_back(**ptr);
        (*ptr)++;
        return out;
    }
    
    while (**ptr != '\0' && (strchr("+-*/(),\t ", **ptr) == NULL)) {
        out.push_back(**ptr);
        (*ptr)++;
    }
    return out;
}

void kgmod::AggrFunc::parse(const string& cmd){
    _original = cmd;
    string work = "(" + cmd + ")";
    char* ptr = (char*)work.c_str();
    vector<char> opeStack;
    vector<pair<string, size_t>> funcStack;
    while (*ptr != '\0') {
        string elem = getOneElem(&ptr);
        if (Cmn::isDigit(ptr)) {
            _stack.push_back({'9', "", stod(elem), 0});
        } else if (elem == "-" && opeStack.back() == '(') {
            _stack.push_back({'9', "", -1, 0});
            opeStack.push_back('*');
        } else if (elem == "+" || (elem == "-" && opeStack.back() != '(')) {
            while ((! opeStack.empty()) && opeStack.back() != '(') {
                _stack.push_back({opeStack.back(), "", 0, 0});
                opeStack.pop_back();
            }
            opeStack.push_back(elem[0]);
        } else if (elem == "*" || elem == "/") {
            while ((! opeStack.empty())  && opeStack.back() != '(' &&
                   (opeStack.back() == '*' || opeStack.back() == '/')) {  // ?????????????
                _stack.push_back({opeStack.back(), "", 0, 0});
                opeStack.pop_back();
            }
            opeStack.push_back(elem[0]);
        } else if (elem == "(") {
            if (! funcStack.empty()) funcStack.back().second++;
            opeStack.push_back(elem[0]);
        } else if (elem == ")") {
            while (opeStack.size() != 0 && opeStack.back() != '(') {
                _stack.push_back({opeStack.back(), "", 0, 0});
                opeStack.pop_back();
            }
            
            if (! funcStack.empty()) {
                funcStack.back().second--;
                if (funcStack.back().second == 0) {
                    _stack.push_back({'F', funcStack.back().first, 0, 0});
                    funcStack.pop_back();
                }
            }
            if (opeStack.size() != 0) opeStack.pop_back();
        } else {
            auto itr1 = _valPosMap->find(elem);
            if (itr1 != _valPosMap->end()) {
                _stack.push_back({'V', "", 0, itr1->second});
                continue;
            }
            
            Cmn::ToUpper(elem);
            if (_funcs.find(elem) != _funcs.end()) {
                funcStack.push_back(make_pair(elem, 1));    // 後ろに"("なければエラーをスローするので、あるのが前提で1をセット
                string nextElem = getOneElem(&ptr);
                if (nextElem != "(") throw kgError("formula error: " + _original);
                opeStack.push_back(nextElem[0]);
                continue;
            }
            throw kgError("unknown data or function: " + _original);
        }
    }
    
    if (! opeStack.empty()) throw kgError("formula error: " + _original);
    
    eval();
    dump();
}

void kgmod::AggrFunc::eval(void) {
    int stackCnt = 0;
    for (const auto& st : _stack) {
        if (st.ope == '9') {
            stackCnt++;
        } else if (st.ope == 'V') {
            stackCnt++;
        } else if (st.ope == 'F') {
            auto f = _funcs.find(st.func);
            if (f == _funcs.end()) throw kgError("syntax error: " + _original);
            if (f->second != 0) stackCnt -= (f->second - 1);
        } else if (strchr("+-*/", st.ope) != NULL) {
            if (stackCnt < 2) throw kgError("syntax error: " + _original);
            stackCnt--;
        }
    }
    if (stackCnt > 1) throw kgError("syntax error: " + _original);
}

double kgmod::AggrFunc::calc(vector<vector<factVal_t>>& dat) {
    vector<vector<factVal_t>> rslt;
    for (const auto& st : _stack) {
        if (st.ope == '9') {
//            vector<factVal_t> buf = {st.val};
            rslt.push_back({st.val});
        } else if (st.ope == 'V') {
//            vector<factVal_t> buf;
//            buf.reserve(dat[st.fldPos].size());
//            copy(dat[st.fldPos].begin(), dat[st.fldPos].end(), back_inserter(buf));
            rslt.push_back(dat[st.fldPos]);
        } else if (st.ope == 'F') {
            if (boost::iequals(st.func, "COUNT")) {
                rslt.push_back({(double)dat[st.fldPos].size()});
                continue;
            }
            
            auto f = _funcs.find(st.func);
            if (f == _funcs.end()) throw kgError("internal error: " + _original);
            if (rslt.size() < f->second) throw kgError("internal error: " + _original);
            
            vector<factVal_t> buf;
            buf.reserve(rslt.back().size());
            copy(rslt.back().begin(), rslt.back().end(), back_inserter(buf));
            
//            vector<factVal_t>& e = rslt.back();
            rslt.back().resize(1);
            if (boost::iequals(st.func, "SUM")) {
                rslt.back()[0] = Cmn::sum(buf);
            } else if (boost::iequals(st.func, "AVE")) {
                rslt.back()[0] = Cmn::mean(buf);
            } else if (boost::iequals(st.func, "MED")) {
                rslt.back()[0] = Cmn::median(buf);
            } else if (boost::iequals(st.func, "MIN")) {
                rslt.back()[0] = Cmn::min(buf);
            } else if (boost::iequals(st.func, "MAX")) {
                rslt.back()[0] = Cmn::max(buf);
            } else if (boost::iequals(st.func, "VAR")) {
                rslt.back()[0] = Cmn::var(buf);
            } else if (boost::iequals(st.func, "SD")) {
                rslt.back()[0] = Cmn::sd(buf);
            } else {
                throw kgError("unknown function: " + _original);
            }
        } else if (strchr("+-*/", st.ope) != NULL) {
            if (rslt.size() < 2) throw kgError("internal error: " + _original);
            vector<factVal_t> buf;
            buf.reserve(rslt.back().size());
            copy(rslt.back().begin(), rslt.back().end(), back_inserter(buf));
            rslt.pop_back();
            
            vector<factVal_t>& e = rslt.back();
            if (e.size() == 1 && buf.size() > 1) {
                e.resize(buf.size());
                for (auto& i : e) i = e[0];
            } else if (e.size() > 1 && buf.size() == 1) {
                buf.resize(e.size());
                for (auto& i : buf) i = buf[0];
            }
            
            if (st.ope == '+') {
                for (size_t i = 0; i < e.size(); i++) e[i] += buf[i];
            } else if (st.ope == '-') {
                for (size_t i = 0; i < e.size(); i++) e[i] -= buf[i];
            } else if (st.ope == '*') {
                for (size_t i = 0; i < e.size(); i++) e[i] *= buf[i];
            } else if (st.ope == '/') {
                for (size_t i = 0; i < e.size(); i++) e[i] /= buf[i];
            }
        }
    }
    
    if ((rslt.size() != 1) && (rslt[0].size() != 1)) {
        throw kgError("internal error: " + _original);
    }
    return rslt[0][0];
}

void kgmod::AggrFunc::dump(void) {
    cerr << _original << endl;
    for (int i = 0; i < _stack.size(); i++) {
        cerr << "[" << i << "] ";
        if (_stack[i].ope == '9') {
            cerr << _stack[i].val << endl;
        } else if (_stack[i].ope == 'V') {
            cerr << "{" << _stack[i].fldPos << "}" << endl;
        } else if (_stack[i].ope == 'F') {
            cerr << _stack[i].func << endl;
        } else {
            cerr << _stack[i].ope << endl;
        }
    }
}


// class FactTable
kgmod::FactTable::FactTable(Config* config, kgEnv* env, Occ* occ)
: _config(config), _env(env), _occ(occ) {
    string dtmDb = config->dbDir + "/fact_table.dtm";
    string occDb = config->dbDir + "/fact_table.dat";
    bmplist.PutDbName(dtmDb, occDb);
    _key2recFile = config->dbDir + "/ft_key2rec.dat";
}

void kgmod::FactTable::item2traBmp(const Ewah& itemBmp, Ewah& traBmp) {
    traBmp.reset();
    for (auto i = itemBmp.begin(), ie = itemBmp.end(); i != ie; i++) {
        string tar = _occ->itemAtt->item[*i];
        Ewah tmp = _occ->bmpList.GetVal(_occ->occKey, tar);
        traBmp = traBmp | tmp;
    }
}

bool kgmod::FactTable::getVals(const size_t trano, const size_t itmno, vals_t*& vals) {
    if (_occ->traAtt->traMax + 1 < trano) return false;
    auto it = _numFldTbl[trano].find(itmno);
    if (it == _numFldTbl[trano].end()) return false;
    vals = &(it->second);
    return true;
}

bool kgmod::FactTable::getItems(const size_t traNo, Ewah& itemBmp) {
    auto ib = _numFldTbl[traNo].begin();
    auto ie = _numFldTbl[traNo].end();
    if (ib == ie) return false;
    for (auto i = ib; i != ie; i++) {
        itemBmp.set(i->first);
    }
    return true;
}

void kgmod::FactTable::build(void) {
    kgCSVfld ft;
    ft.open(_config->traFile.name, _env, false);
    ft.read_header();
    
    cerr << "building transaction and fact index" << endl;
    vector<string> fldName = ft.fldName();
    int traIDPos = -1, itemIDPos = -1;
    size_t factCnt = 0;
    string traIDFld = _config->traFile.traFld;
    string itemIDFld = _config->traFile.itemFld;
    for (int i = 0; i < fldName.size(); i++) {
        if (fldName[i] == _config->traFile.traFld) {
            traIDPos = i;
        } else if (fldName[i] == _config->traFile.itemFld) {
            itemIDPos = i;
        } else {
            if (Cmn::posInVector(_config->traFile.strFields, fldName[i])) {
                if (Cmn::posInVector(_config->traFile.highCardinality, fldName[i])) {
                    bmplist.InitKey(fldName[i], STR_HC);
                } else {
                    bmplist.InitKey(fldName[i], STR);
                }
                factCnt++;
            } else if (auto pos = Cmn::posInVector(_config->traFile.numFields, fldName[i])) {
                _numFldPos[fldName[i]] = (int)(*pos);
                
                if (Cmn::posInVector(_config->traFile.highCardinality, fldName[i])) {
                    bmplist.InitKey(fldName[i], NUM_HC);
                } else {
                    bmplist.InitKey(fldName[i], NUM);
                }
                factCnt++;
            }
        }
    }
    
    if (traIDPos == -1) {
        stringstream ss;
        ss << traIDFld << " is not found on " << _config->traFile.name;
        throw kgError(ss.str());
    }
    if (itemIDPos == -1) {
        stringstream ss;
        ss << itemIDFld << " is not found on " << _config->traFile.name;
        throw kgError(ss.str());
    }
    
    //if (factCnt == 0) return;
    
    //
    map<string, bool> checkedTra;
    for (auto i = _occ->traAtt->traNo.begin(), ie = _occ->traAtt->traNo.end(); i != ie; i++) {
        checkedTra[i->first] = false;
    }
    
    _numFldTbl.resize(_occ->traAtt->traMax + 1);
    bool isError = false;
    set<string> errKeyList;
    while (ft.read() != EOF) {
        string traID = ft.getVal(traIDPos);
        size_t traNo = _occ->traAtt->traNo[traID];
        string itemID = ft.getVal(itemIDPos);
        size_t itemNo = _occ->itemAtt->itemNo[itemID];
        _numFldTbl[traNo][itemNo].resize(_numFldPos.size());
        
        bool errThisTime = false;
        for (int i = 0; i < fldName.size(); i++) {
            if (i == traIDPos) {
                // traNoがtraAttに登録されていなければエラーとするが，処理は全て実施する
                if (_occ->traAtt->traNo.find(traID) == _occ->traAtt->traNo.end()) {
                    isError = true;
                    string buf = traIDFld + ":" + traID;
                    if (errKeyList.find(buf) == errKeyList.end()) {
                        stringstream ss;
                        ss << "#ERROR# " << buf << " is not found on " << _config->traAttFile.name;
                        cerr << ss.str() << endl;
                        errKeyList.insert(buf);
                    }
                    errThisTime = true;
                }
                continue;
            }
            if (i == itemIDPos) {
                // itemNoがitemAttに登録されていなければエラーとするが，処理は全て実施する
                if (_occ->itemAtt->itemNo.find(itemID) == _occ->itemAtt->itemNo.end()) {
                    isError = true;
                    string buf = itemIDFld + ":" + itemID;
                    if (errKeyList.find(buf) == errKeyList.end()) {
                        stringstream ss;
                        ss << "#ERROR# " << buf << " is not found on " << _config->itemAttFile.name;
                        cerr << ss.str() << endl;
                        errKeyList.insert(buf);
                    }
                    errThisTime = true;
                }
                continue;
            }
            // configファイルに登録されていないデータ項目は処理しない
            if (!Cmn::posInVector(_config->traFile.strFields, fldName[i]) &&
                !Cmn::posInVector(_config->traFile.numFields, fldName[i])) continue;
            
            string fld = fldName[i];
            string val = ft.getVal(i);
            if (auto p = Cmn::posInVector(_config->traFile.numFields, fld)) {
                _numFldTbl[traNo][itemNo][*p] = stod(ft.getVal(i));
            }
            bmplist.SetBit(fld, val, ft.recNo() - 1);
        }
        _occ->bmpList.SetBit(_occ->occKey, itemID, _occ->traAtt->traNo[traID]);
        if (_occ->traAtt->traNo[traID] >= _occ->occ.size()) _occ->occ.resize(_occ->traAtt->traNo[traID] + 1);
        Ewah tmp;
        tmp.set(_occ->itemAtt->itemNo[itemID]);
        _occ->occ[_occ->traAtt->traNo[traID]] = _occ->occ[_occ->traAtt->traNo[traID]] | tmp;
        checkedTra[traID] = true;
        
        _addr.push_back({traNo, itemNo});               // it means {traNo, itemNo} set to [ft.recNo() - 1]
        size_t rn = ft.recNo() - 1;
        _recNo.insert({{traNo, itemNo}, rn});
    }
    recMax = ft.recNo() - 1;
    ft.close();
    
    _occ->liveTra.padWithZeroes(_occ->traAtt->traMax + 1); _occ->liveTra.inplace_logicalnot();
    for (auto i = checkedTra.begin(), ie = checkedTra.end(); i != ie; i++) {
        if (i->second) continue;
        cerr << "#WARNING# " << traIDFld << ":" << i->first << " does not exist on " << _config->traFile.name << endl;
        
        Ewah tmp; tmp.set(_occ->traAtt->traNo[i->first]);
        _occ->liveTra = _occ->liveTra - tmp;
    }
    
    if (isError) throw kgError("error occurred in building transaction index");
}

void kgmod::FactTable::save(const bool clean = true) {
    bmplist.save(clean);
    
    cerr << "writing " << _key2recFile << " ..." << endl;
    FILE* fp = fopen(_key2recFile.c_str(), "wb");
    if (fp == NULL) {
        stringstream msg;
        msg << "failed to open " + _key2recFile;
        throw kgError(msg.str());
    }
    
    try {
        size_t rc;
        size_t fldCnt = _config->traFile.numFields.size();
        rc = fwrite(&fldCnt, sizeof(size_t), 1, fp);
        if (rc == 0) throw 0;
        
        for (size_t i = 0; i < _addr.size(); i++) {
            size_t traNo = _addr[i].first;
            size_t itemNo = _addr[i].second;
            
            rc = fwrite(&i, sizeof(size_t), 1, fp);
            if (rc == 0) throw 0;
            rc = fwrite(&traNo, sizeof(size_t), 1, fp);
            if (rc == 0) throw 0;
            rc = fwrite(&itemNo, sizeof(size_t), 1, fp);
            if (rc == 0) throw 0;
            
            for (size_t c = 0; c < fldCnt; c++) {
                rc = fwrite(&(_numFldTbl[traNo][itemNo][c]), sizeof(factVal_t), 1, fp);
                if (rc == 0) throw 0;
            }
        }
    }
    catch(int e) {
        fclose(fp);
        stringstream msg;
        msg << "failed to write " << _key2recFile;
        throw kgError(msg.str());
    }
    fclose(fp);
}

void kgmod::FactTable::load(void) {
    cerr << "loading fact table" << endl;
    bmplist.load();
    //    bmplist.dump(true);
    
    for (int pos = 0; pos < _config->traFile.numFields.size(); pos++) {
        _numFldPos[_config->traFile.numFields[pos]] = pos;
    }
    
    cerr << "reading " << _key2recFile << "..." << endl;
    _numFldTbl.resize(_occ->traAtt->traMax + 1);
    FILE* fp = fopen(_key2recFile.c_str(), "rb");
    if (fp == NULL) {
        stringstream msg;
        msg << "failed to open " + _key2recFile;
        throw kgError(msg.str());
    }
    try {
        size_t fldCnt;
        size_t rc = 0;
        rc = fread(&fldCnt, sizeof(size_t), 1, fp);
        while (true) {
            size_t recNo = 0;
            size_t traNo = 0;
            size_t itemNo = 0;
            size_t rc = 0;
            rc = fread(&recNo, sizeof(size_t), 1, fp);
            if (rc == 0) break;
            rc = fread(&traNo, sizeof(size_t), 1, fp);
            if (rc == 0) throw 0;
            rc = fread(&itemNo, sizeof(size_t), 1, fp);
            if (rc == 0) throw 0;
            
            _addr.push_back({traNo, itemNo});
            _recNo.insert({{traNo, itemNo}, recNo});
            _numFldTbl[traNo][itemNo].resize(fldCnt);
            for (size_t c = 0; c < fldCnt; c++) {
                rc = fread(&(_numFldTbl[traNo][itemNo][c]), sizeof(factVal_t), 1, fp);
                if (rc == 0) throw 0;
            }
            recMax = recNo;
        }
    }
    catch(int e) {
        fclose(fp);
        stringstream msg;
        msg << "failed to read " << _key2recFile;
        throw kgError(msg.str());
    }
    fclose(fp);
}

void kgmod::FactTable::toTraItemBmp(const Ewah& factFilter, const Ewah& itemFilter,
                                    Ewah& traBmp, Ewah& itemBmp) {
    set<size_t> traWork;
    set<size_t> itemWork;
    for (auto i = factFilter.begin(), ei = factFilter.end(); i != ei; i++) {
        if (itemFilter.get(_addr[*i].second)) {
            traWork.insert(_addr[*i].first);
            itemWork.insert(_addr[*i].second);
        }
    }
    
    traBmp.reset();
    for (auto t : traWork) {
        traBmp.set(t);
    }
    
    itemBmp.reset();
    for (auto t : itemWork) {
        itemBmp.set(t);
    }
}

//void kgmod::FactTable::toItemBmp(const Ewah& factFilter, const Ewah& traFilter,
//                                 const Ewah& itemFilter, Ewah& itemBmp) {
//    set<size_t> work;
//    for (auto i = factFilter.begin(), ei = factFilter.end(); i != ei; i++) {
//        if (!itemFilter.get(_addr[*i].second)) continue;
//        if (!traFilter.get(_addr[*i].first)) continue;
//        work.insert(_addr[*i].second);
//    }
//
//    itemBmp.reset();
//    for (auto t : work) {
//        itemBmp.set(t);
//    }
//}

string kgmod::FactTable::valNames(void) {
    string out;
    for (auto& fld : _config->traFile.numFields) {
        out += fld + ",";
    }
    out.erase(--out.end());
    return out;
}

void kgmod::FactTable::funcParse(const string& func, vector<string>& f) {
    // count(f)
    vector<string> vec = Cmn::Split(func, '(');
    f.push_back(vec[0]);
    boost::trim(vec[0]);
    auto it = vec[1].end();
    if (*it != ')') throw kgError("syntax error: " + func);
    vec[1].erase(--it);
    vector<string> vecParam = Cmn::CsvStr::Parse(vec[1]);
    for (auto& param : vecParam) f.push_back(param);
}

boost::optional<int> kgmod::FactTable::fldPos(const string fld) {
    auto it = _numFldPos.find(fld);
    if (it == _numFldPos.end()) return boost::none;
    else return it->second;
}

size_t kgmod::FactTable::aggregate(const pair<string&, Ewah&>& traBmp, const pair<string&, Ewah&>& itemBmp,
                                   vector<pair<AggrFunc, string>>& vals, string& line) {
    size_t skipCount0 = 0, skipCount = 0, hitCount = 0;
    line.clear();
    vector<vals_t> factVals(_config->traFile.numFields.size());
    for (size_t p = 0; p < factVals.size(); p++) {
        factVals[p].reserve(256);
    }
    for (auto i = itemBmp.second.begin(), ei = itemBmp.second.end(); i != ei; i++) {
        Ewah* tmpBmp;
        if (! _occ->bmpList.GetVal(_occ->occKey, _occ->itemAtt->item[*i], tmpBmp)) {
            skipCount0++;
            continue;
        }
        Ewah tarTraBmp = *tmpBmp & traBmp.second;
        for (auto t = tarTraBmp.begin(), et = tarTraBmp.end(); t != et; t++) {
            vals_t* fvals;
            if (! getVals(*t, *i, fvals)) {
                skipCount++;
                continue;
            }
            hitCount++;
            for (size_t p = 0; p < fvals->size(); p++) {
                factVals[p].push_back((*fvals)[p]);
            }
        }
    }
    if (factVals[0].empty()) {
//        cerr << "0 ";
        return 0;
    }
    
    size_t lineCount = 0;
    if (vals.empty()) {
        for (size_t i = 0; i < factVals[0].size(); i++) {
            char buf[2048];
            if (lineCount == 0) {
                sprintf(buf, "%s,%s", traBmp.first.c_str(), itemBmp.first.c_str());
            } else {
                sprintf(buf, "\n%s,%s", traBmp.first.c_str(), itemBmp.first.c_str());
            }
            for (size_t j = 0; j < factVals.size(); j++) {
                char tmp[256];
                factVal_t v = factVals[j][i];
                if (v == (int)v) {
                    sprintf(tmp, ",%d", (int)v);
                } else {
                    sprintf(tmp, ",%f", v);
                }
                strcat(buf, tmp);
            }
            line += string(buf);
            lineCount++;
        }
    } else {
        char buf[2048];
        if (traBmp.first == "") {
            sprintf(buf, "%s", itemBmp.first.c_str());
        } else {
            sprintf(buf, "%s,%s", traBmp.first.c_str(), itemBmp.first.c_str());
        }
        
        for (size_t i = 0; i < vals.size(); i++) {
            char tmp[256]; memset(tmp, '\0', sizeof(tmp));
            factVal_t v = vals[i].first.calc(factVals);
            if (v == -DBL_MAX) {
                strcpy(tmp, ",err");
            } else {
                if (v == (int)v) {
                    sprintf(tmp, ",%d", (int)v);
                } else {
                    sprintf(tmp, ",%f", v);
                }
            }
            strcat(buf, tmp);
        }
        line = string(buf);
        lineCount++;
    }
    cerr << "(" << skipCount0 << "," << skipCount << "," << hitCount << ":" << lineCount << ") " << endl;
    return lineCount;
}

//void kgmod::FactTable::toTraBitmap(const Ewah& factBmp, Ewah& traBmp) {
//    unordered_map<size_t, bool> checked_traNo;
//    for (auto i = factBmp.begin(), ei= factBmp.end(); i != ei; i++) {
//        if (checked_traNo.find(*i) != checked_traNo.end()) continue;
//        traBmp.set(*i);
//        checked_traNo[*i] = true;
//    }
//}

//void kgmod::FactTable::fact2traNo(const size_t traNo, Ewah& factBmp) {
//    set<size_t> traNoSet;
//    for (auto i = _recNo.lower_bound({traNo, 0}); i != _recNo.end(); i++) {
//        if (traNo != i->first.first) break;
//        traNoSet.insert(i->second);
//    }
//    
//    for (auto i = traNoSet.begin(); i != traNoSet.end(); i++) {
//        factBmp.set(*i);
//    }
//}
//
//void kgmod::FactTable::itemsInFact(const size_t traNo, const Ewah& factBmp, Ewah& itemBmp) {
//    Ewah fact4TraNo;
//    fact2traNo(traNo, fact4TraNo);
//    Ewah tmpBmp = fact4TraNo & factBmp;
//
//    set<size_t> itemNoSet;
//    for (auto i = tmpBmp.begin(), ei = tmpBmp.end(); i != ei; i++) {
//        auto buf = _addr[*i];
//        itemNoSet.insert(buf.second);
//    }
//
//    for (auto i = itemNoSet.begin(); i != itemNoSet.end(); i++) {
//        itemBmp.set(*i);
//    }
//}

//bool kgmod::FactTable::existTra(const Ewah& factBmp, const size_t traNo) {
//    Ewah bmp4tra;
//    fact2traNo(traNo, bmp4tra);
//    Ewah comp = factBmp & bmp4tra;
//    size_t cnt = comp.numberOfOnes();
//    return (cnt != 0);
//}

void kgmod::FactTable::dump(void) {
    string dmpfile = _config->outDir + "/facttable.dmp";
    ofstream ofs(dmpfile);
    
    ofs << "numField" << endl;
//    ofs << "_numFldPos,_config->traFile.numFields: ";
    for (size_t i = 0; i < _config->traFile.numFields.size(); i++) {
        ofs << _numFldPos[_config->traFile.numFields[i]] << "," << _config->traFile.numFields[i] << " ";
    }
    ofs << endl;
    
    ofs << "\nstrField" << endl;
//    ofs << "_numFldPos,_config->traFile.numFields: ";
    for (size_t i = 0; i < _config->traFile.strFields.size(); i++) {
        ofs << _config->traFile.strFields[i] << " ";
    }
    ofs << endl;
    
    ofs << "\n_numFldTbl" << endl;
    for (size_t i1 = 0; i1 < _numFldTbl.size(); i1++) {
        string traID = _occ->traAtt->tra[i1];
        for (auto i2 = _numFldTbl[i1].begin(); i2 != _numFldTbl[i1].end(); i2++) {
            string itemID = _occ->itemAtt->item[i2->first];
            ofs << traID << "," << itemID << ": ";
            for (auto f : i2->second) {
                ofs << f << " ";
            }
            ofs << endl;
        }
    }
    ofs << endl;
    
    ofs << "\n_addr" << endl;
    for (size_t i = 0; i < _addr.size(); i++) {
        ofs << i << ": " << _addr[i].first << "," << _addr[i].second;
        ofs << "(" << _occ->traAtt->tra[_addr[i].first] << ",";
        ofs << _occ->itemAtt->item[_addr[i].second] << ")" << endl;
    }
    ofs << endl;
    
    ofs << "\n_recNo" << endl;
    for (auto i = _recNo.begin(), ei = _recNo.end(); i != ei; i++) {
        ofs << i->first.first << "," << i->first.second;
        ofs << "(" << _occ->traAtt->tra[i->first.first] << ",";
        ofs << _occ->itemAtt->item[i->first.second] << "): ";
        ofs << i->second << endl;
    }
    
    ofs.close();
//    bmplist.dump(true);
}
