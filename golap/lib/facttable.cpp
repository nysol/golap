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
    auto it = _factTable[trano].find(itmno);
    if (it == _factTable[trano].end()) return false;
    vals = &(it->second);
    return true;
}

bool kgmod::FactTable::getItems(const size_t traNo, Ewah& itemBmp) {
    auto ib = _factTable[traNo].begin();
    auto ie = _factTable[traNo].end();
    if (ib == ie) return false;
    for (auto i = ib; i != ie; i++) {
        itemBmp.set(i->first);
    }
    return true;
}

void kgmod::FactTable::load(void) {
    cerr << "loadding fact table" << endl;
    kgCSVfld ft;
    ft.open(_config->traFile.name, _env, false);
    ft.read_header();
    vector<string> fldName = ft.fldName();
    int pos = 0, traIDPos = 0, itemIDPos = 0;
    for (int i = 0; i < fldName.size(); i++) {
        if (fldName[i] == _config->traFile.traFld) {
            traIDPos = i;
        } else if (fldName[i] == _config->traFile.itemFld) {
            itemIDPos = i;
        } else {
            _flds.push_back(fldName[i]);
            _fldPos[fldName[i]] = pos;
            pos++;
        }
    }
    if (_fldPos.empty()) return;
    
    _factTable.resize(_occ->traAtt->traMax + 1);
    while (ft.read() != EOF) {
        string traID = ft.getVal(traIDPos);
        size_t traNo  = _occ->traAtt->traNo[traID];
        string itemID = ft.getVal(itemIDPos);
        size_t itemNo = _occ->itemAtt->itemNo[itemID];
        _factTable[traNo][itemNo].reserve(_fldPos.size());
        for (int i = 0; i < fldName.size(); i++) {
            if (i == traIDPos) continue;
            if (i == itemIDPos) continue;
            factVal_t val = stof(ft.getVal(i));
            _factTable[traNo][itemNo].push_back(val);
        }
    }
    ft.close();
}

string kgmod::FactTable::valNames(void) {
    string out;
    for (auto& fld : _flds) {
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
    auto it = _fldPos.find(fld);
    if (it == _fldPos.end()) return boost::none;
    else return it->second;
}

size_t kgmod::FactTable::aggregate(const pair<string&, Ewah&>& traBmp, const pair<string&, Ewah&>& itemBmp,
                                   vector<pair<AggrFunc, string>>& vals, string& line) {
    size_t skipCount0 = 0, skipCount = 0, hitCount = 0;
    line.clear();
    vector<vals_t> factVals(_flds.size());
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
