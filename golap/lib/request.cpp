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

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/optional.hpp>
#include <boost/exception/all.hpp>
#include <kgError.h>
#include "filter.hpp"
#include "request.hpp"

using namespace std;
using namespace kgmod;

Dimension kgmod::Request::makeDimBitmap(string& cmdline) {
    boost::trim(cmdline);
    Dimension out;
    if (cmdline == "") {
        out.key = "";
        return out;
    }
    vector<string> param = Cmn::CsvStr::Parse(cmdline);
    out.key = param[0];
    for (size_t i = 1; i < param.size(); i++) {
        out.DimBmpList[param[i]] = _occ->bmpList.GetVal(out.key, param[i]);
    }
    return out;
}

void kgmod::Request::setQueryDefault(void) {
    query.traFilter.padWithZeroes(_occ->traAtt->traMax + 1);
    query.traFilter.inplace_logicalnot();
    query.itemFilter.padWithZeroes(_occ->itemAtt->itemMax + 1);
    query.itemFilter.inplace_logicalnot();
    query.selCond = {0, 0, 0, 0, -1};
    query.debug_mode = 0;
    query.granularity.first.resize(1);
    query.granularity.first[0] = _config->traFile.traFld;
    query.granularity.second.resize(1);
    query.granularity.second[0] = _config->traFile.itemFld;
    query.dimension.key = "";
    query.dimension.DimBmpList.clear();
    query.sendMax = _config->sendMax;
}

void kgmod::Request::setNodestatDefault(void) {
    nodestat.traFilter.padWithZeroes(_occ->traAtt->traMax + 1);
    nodestat.traFilter.inplace_logicalnot();
    nodestat.itemFilter.padWithZeroes(_occ->itemAtt->itemMax + 1);
    nodestat.itemFilter.inplace_logicalnot();
    nodestat.granularity.first.resize(1);
    nodestat.granularity.first[0] = _config->traFile.traFld;
    nodestat.granularity.second.resize(1);
    nodestat.granularity.second[0] = _config->traFile.itemFld;
}

void kgmod::Request::setNodeimageDefault(void) {
    nodeimage.traFilter.padWithZeroes(_occ->traAtt->traMax + 1);
    nodeimage.traFilter.inplace_logicalnot();
    nodeimage.itemFilter.padWithZeroes(_occ->itemAtt->itemMax + 1);
    nodeimage.itemFilter.inplace_logicalnot();
    nodeimage.granularity.first.resize(1);
    nodeimage.granularity.first[0] = _config->traFile.traFld;
    nodeimage.granularity.second.resize(1);
    nodeimage.granularity.second[0] = _config->traFile.itemFld;
}

void kgmod::Request::evalRequestJson(string& req_msg) {
    cerr << "request: " << req_msg << endl;
    stringstream json(req_msg);
    boost::property_tree::ptree pt;
    try {
        boost::property_tree::read_json(json, pt);
        //    cerr << "request: " << endl;
        //    boost::property_tree::write_json(cerr, pt, true);
    }
    catch(const boost::exception& e) {
        string diag = diagnostic_information(e);
        cerr << diag << endl;
        throw kgError("json parse error");
    }
    
    if (boost::optional<unsigned int> val = pt.get_optional<unsigned int>("deadlineTimer")) {
        deadlineTimer = *val;
    } else {
        deadlineTimer = _config->deadlineTimer;
    }
    
    if (boost::optional<string> val = pt.get_optional<string>("control")) {
        mode = "control";
        etcRec.func = *val;
    } else if (boost::optional<string> val = pt.get_optional<string>("retrieve")) {
        mode = "retrieve";
        vector<string> vec = Cmn::CsvStr::Parse(*val);
        etcRec.args.reserve(vec.size() - 1);
        etcRec.func = vec[0];
        for (size_t i = 1; i < vec.size(); i++) {
            etcRec.args.push_back(vec[i]);
        }
    } else if (boost::optional<string> val = pt.get_optional<string>("query")) {
        mode = "query";
        setQueryDefault();
        if (boost::optional<string> val2 = pt.get_optional<string>("query.traFilter")) {
            query.traFilter = _filter->makeTraBitmap(*val2);
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("query.itemFilter")) {
            query.itemFilter = _filter->makeItemBitmap(*val2);
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("query.selCond")) {
            if (boost::optional<double> val3 = pt.get_optional<double>("query.selCond.minSup")) {
                query.selCond.minSup = *val3;
            }
            if (boost::optional<double> val3 = pt.get_optional<double>("query.selCond.minConf")) {
                query.selCond.minConf = *val3;
            }
            if (boost::optional<double> val3 = pt.get_optional<double>("query.selCond.minLift")) {
                query.selCond.minLift = *val3;
            }
            if (boost::optional<double> val3 = pt.get_optional<double>("query.selCond.minJac")) {
                query.selCond.minJac = *val3;
            }
            if (boost::optional<double> val3 = pt.get_optional<double>("query.selCond.minPMI")) {
                query.selCond.minPMI = *val3;
            }
            query.selCond.dump();
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("query.sortKey")) {
            if (boost::iequals(*val2, "SUP")) {
                query.sortKey = SORT_SUP;
            } else if (boost::iequals(*val2, "CONF")) {
                query.sortKey = SORT_CONF;
            } else if (boost::iequals(*val2, "LIFT")) {
                query.sortKey = SORT_LIFT;
            } else if (boost::iequals(*val2, "JAC")) {
                query.sortKey = SORT_JAC;
            } else if (boost::iequals(*val2, "PMI")) {
                query.sortKey = SORT_PMI;
            } else {
                query.sortKey = SORT_NONE;
            }
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("query.sendMax")) {
            query.sendMax = stoi(*val2);
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("query.granularity")) {
            if (boost::optional<string> val3 = pt.get_optional<string>("query.granularity.transaction")) {
                vector<string> buf = Cmn::CsvStr::Parse((*val3).c_str());
                if (! buf.empty()) {
                    query.granularity.first.clear();
                    query.granularity.first.reserve(buf.size());
                    for (auto& f : buf) {
                        if (Cmn::posInVector(_config->traAttFile.granuFields, f)) {
                            query.granularity.first.push_back(f);
                        } else {
                            string msg = "query.granularity.transaction(";
                            msg += *val3;
                            msg += ") must be set in config file (traAttFile.granuFields)\n";
                            throw kgError(msg);
                        }
                    }
                }
            }
            if (boost::optional<string> val3 = pt.get_optional<string>("query.granularity.node")) {
                vector<string> buf = Cmn::CsvStr::Parse((*val3).c_str());
                if (! buf.empty()) {
                    query.granularity.second.clear();
                    query.granularity.second.reserve(buf.size());
                    for (auto& f : buf) {
                        if (! _occ->itemAtt->isItemAtt(f)) {
                            string msg = f;
                            msg += " is not item attribute\n";
                            throw kgError(msg);
                        }
                        query.granularity.second.push_back(f);
                    }
                }
            }
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("query.dimension")) {
            query.dimension = makeDimBitmap(*val2);
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("query.debugMode")) {
            // デバッグ用
            if (boost::iequals(*val2, "notExec")) {
                query.debug_mode = 1;
                string msg = *val2 + "\n";
                throw kgError(msg);
            } else if (boost::iequals(*val2, "checkData")) {
                query.debug_mode = 2;
            }
        }
    } else if (boost::optional<string> val = pt.get_optional<string>("nodestat")) {
        mode = "nodestat";
        setNodestatDefault();
        if (boost::optional<string> val2 = pt.get_optional<string>("nodestat.traFilter")) {
            nodestat.traFilter = _filter->makeTraBitmap(*val2);
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("nodestat.itemFilter")) {
            nodestat.itemFilter = _filter->makeItemBitmap(*val2);
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("nodestat.granularity")) {
            if (boost::optional<string> val3 = pt.get_optional<string>("nodestat.granularity.transaction")) {
                vector<string> buf = Cmn::CsvStr::Parse((*val3).c_str());
                if (! buf.empty()) {
                    nodestat.granularity.first.clear();
                    nodestat.granularity.first.reserve(buf.size());
                    for (auto& f : buf) {
                        if (Cmn::posInVector(_config->traAttFile.granuFields, f)) {
                            nodestat.granularity.first.push_back(f);
                        } else {
                            string msg = "nodestat.granularity.transaction(";
                            msg += *val3;
                            msg += ") must be set in config file (traAttFile.granuFields)\n";
                            throw kgError(msg);
                        }
                    }
                }
            }
            if (boost::optional<string> val3 = pt.get_optional<string>("nodestat.granularity.node")) {
                vector<string> buf = Cmn::CsvStr::Parse((*val3).c_str());
                if (! buf.empty()) {
                    nodestat.granularity.second.clear();
                    nodestat.granularity.second.reserve(buf.size());
                    for (auto& f : buf) {
                        if (! _occ->itemAtt->isItemAtt(f)) {
                            string msg = f;
                            msg += " is not item attribute\n";
                            throw kgError(msg);
                        }
                        nodestat.granularity.second.push_back(f);
                    }
                }
            }
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("nodestat.itemVal")) {
            vector<string> buf = Cmn::CsvStr::Parse((*val2).c_str());
            nodestat.itemVal.reserve(buf.size());
            for (auto& f : buf) {
                nodestat.itemVal.push_back(f);
            }
        } else {
            string msg = "nodestat.itemVal must be set in request\n";
            throw kgError(msg);
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("nodestat.values")) {
            vector<string> vec = Cmn::CsvStr::Parse(*val2);
            nodestat.vals.resize(vec.size());
            for (size_t i = 0; i < vec.size(); i++) {
                nodestat.vals[i].first.setValPosMap(_factTable->valPosMap());
                vector<string>v = Cmn::Split(vec[i], ':');
                nodestat.vals[i].first.parse(v[0]);
                if (v.size() == 1) {
                    nodestat.vals[i].second = v[0];
                } else if (v.size() == 2) {
                    nodestat.vals[i].second = v[1];
                } else {
                    throw kgError("error in nodestat.values");
                }
            }
        }
    } else if (boost::optional<string> val = pt.get_optional<string>("nodeimage")) {
        mode = "nodeimage";
        if (_config->itemAttFile.imageField.empty()) {
            throw kgError("imageField is not set in config file");
        }
        setNodeimageDefault();
        if (boost::optional<string> val2 = pt.get_optional<string>("nodeimage.traFilter")) {
            nodeimage.traFilter = _filter->makeTraBitmap(*val2);
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("nodeimage.itemFilter")) {
            nodeimage.itemFilter = _filter->makeItemBitmap(*val2);
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("nodeimage.granularity")) {
            if (boost::optional<string> val3 = pt.get_optional<string>("nodeimage.granularity.transaction")) {
                vector<string> buf = Cmn::CsvStr::Parse((*val3).c_str());
                if (! buf.empty()) {
                    nodeimage.granularity.first.clear();
                    nodeimage.granularity.first.reserve(buf.size());
                    for (auto& f : buf) {
                        if (Cmn::posInVector(_config->traAttFile.granuFields, f)) {
                            nodeimage.granularity.first.push_back(f);
                        } else {
                            string msg = "nodeimage.granularity.transaction(";
                            msg += *val3;
                            msg += ") must be set in config file (traAttFile.granuFields)\n";
                            throw kgError(msg);
                        }
                    }
                }
            }
            if (boost::optional<string> val3 = pt.get_optional<string>("nodeimage.granularity.node")) {
                vector<string> buf = Cmn::CsvStr::Parse((*val3).c_str());
                if (! buf.empty()) {
                    nodeimage.granularity.second.clear();
                    nodeimage.granularity.second.reserve(buf.size());
                    for (auto& f : buf) {
                        if (! _occ->itemAtt->isItemAtt(f)) {
                            string msg = f;
                            msg += " is not item attribute\n";
                            throw kgError(msg);
                        }
                        nodeimage.granularity.second.push_back(f);
                    }
                }
            }
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("nodeimage.itemVal")) {
            vector<string> buf = Cmn::CsvStr::Parse((*val2).c_str());
            nodeimage.itemVal.reserve(buf.size());
            for (auto& f : buf) {
                nodeimage.itemVal.push_back(f);
            }
        } else {
            string msg = "nodeimage.itemVal must be set in request\n";
            throw kgError(msg);
        }
    } else if (boost::optional<string> val = pt.get_optional<string>("worksheet")) {
        mode = "worksheet";
        if (boost::optional<string> val2 = pt.get_optional<string>("worksheet.traFilter")) {
            worksheet.traFilter = _filter->makeTraBitmap(*val2);
        } else {
            worksheet.traFilter.padWithZeroes(_occ->traAtt->traMax + 1);
            worksheet.traFilter.inplace_logicalnot();
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("worksheet.itemFilter")) {
            worksheet.itemFilter = _filter->makeItemBitmap(*val2);
        } else {
            worksheet.itemFilter.padWithZeroes(_occ->itemAtt->itemMax + 1);
            worksheet.itemFilter.inplace_logicalnot();
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("worksheet.traAtt")) {
            vector<string> vec = Cmn::CsvStr::Parse(*val2);
            worksheet.traAtt.resize(vec.size());
            for (size_t i = 0; i < vec.size(); i++) {
                worksheet.traAtt[i].first = 'T';
                worksheet.traAtt[i].second = vec[i];
            }
        } else {
            throw kgError("worksheet.traAtt must be set");
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("worksheet.itemAtt")) {
            vector<string> vec = Cmn::CsvStr::Parse(*val2);
            worksheet.itemAtt.resize(vec.size());
            for (size_t i = 0; i < vec.size(); i++) {
                worksheet.itemAtt[i].first = 'I';
                worksheet.itemAtt[i].second = vec[i];
            }
        } else {
            throw kgError("worksheet.itemAtt must be set");
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("worksheet.values")) {
            vector<string> vec = Cmn::CsvStr::Parse(*val2);
            worksheet.vals.resize(vec.size());
            for (size_t i = 0; i < vec.size(); i++) {
                worksheet.vals[i].first.setValPosMap(_factTable->valPosMap());
                vector<string>v = Cmn::Split(vec[i], ':');
                worksheet.vals[i].first.parse(v[0]);
                if (v.size() == 1) {
                    worksheet.vals[i].second = v[0];
                } else if (v.size() == 2) {
                    worksheet.vals[i].second = v[1];
                } else {
                    throw kgError("error in worksheet.values");
                }
            }
        }
    } else if (boost::optional<string> val = pt.get_optional<string>("pivot")) {
        mode = "pivot";
        if (boost::optional<string> val2 = pt.get_optional<string>("pivot.traFilter")) {
            pivot.traFilter = _filter->makeTraBitmap(*val2);
        } else {
            pivot.traFilter.padWithZeroes(_occ->traAtt->traMax + 1);
            pivot.traFilter.inplace_logicalnot();
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("pivot.itemFilter")) {
            pivot.itemFilter = _filter->makeTraBitmap(*val2);
        } else {
            pivot.itemFilter.padWithZeroes(_occ->itemAtt->itemMax + 1);
            pivot.itemFilter.inplace_logicalnot();
        }
        pivot.axes.resize(2);
        if (boost::optional<string> val2 = pt.get_optional<string>("pivot.colAtt")) {
            vector<string>csv = Cmn::CsvStr::Parse(*val2);
            pivot.axes[0].reserve(csv.size());
            for (auto& i : csv) {
                vector<string>vec = Cmn::Split(i, ':');
                if (vec.size() == 1) {
                    if (! Cmn::posInVector(_config->traAttFile.strFields, vec[0])) {
//                        (! Cmn::posInVector(_config->traAttFile.catFields, vec[0]))) {
                        string msg = vec[0] + " is not transaction attribute\n";
                        throw kgError(msg);
                    }
                    pivot.axes[0].push_back({'T', vec[0]});
                } else if (vec.size() >= 2) {
                    transform(vec[0].cbegin(), vec[0].cend(), vec[0].begin(), ::toupper);
                    string msg;
                    if (vec[0][0] == 'T') {
                        if (! Cmn::posInVector(_config->traAttFile.strFields, vec[1])) {
//                            (! Cmn::posInVector(_config->traAttFile.catFields, vec[1]))) {
                            msg = vec[1] + " is not transaction attribute\n";
                            throw kgError(msg);
                        }
                    } else if (vec[0][0] == 'I') {
                        if (! Cmn::posInVector(_config->itemAttFile.strFields, vec[1])) {
                            msg = vec[1] + " is not transaction attribute\n";
                            throw kgError(msg);
                        }
                    } else {
                        msg = "illegular format: " + i + "\n";
                        throw kgError(msg);
                    }
                    pivot.axes[0].push_back({vec[0][0], vec[1]});
                }
            }
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("pivot.rowAtt")) {
            vector<string>csv = Cmn::CsvStr::Parse(*val2);
            pivot.axes[1].reserve(csv.size());
            for (auto& i : csv) {
                vector<string>vec = Cmn::Split(i, ':');
                if (vec.size() == 1) {
                    if (! Cmn::posInVector(_config->traAttFile.strFields, vec[0])) {
//                        (! Cmn::posInVector(_config->traAttFile.catFields, vec[0]))) {
                        string msg = vec[0] + " is not transaction attribute\n";
                        throw kgError(msg);
                    }
                    pivot.axes[1].push_back({'T', vec[0]});
                } else if (vec.size() >= 2) {
                    transform(vec[0].cbegin(), vec[0].cend(), vec[0].begin(), ::toupper);
                    string msg;
                    if (vec[0][0] == 'T') {
                        if (! Cmn::posInVector(_config->traAttFile.strFields, vec[1])) {
//                            (! Cmn::posInVector(_config->traAttFile.catFields, vec[1]))) {
                            msg = vec[1] + " is not transaction attribute\n";
                            throw kgError(msg);
                        }
                    } else if (vec[0][0] == 'I') {
                        if (! Cmn::posInVector(_config->itemAttFile.strFields, vec[1])) {
                            msg = vec[1] + " is not transaction attribute\n";
                            throw kgError(msg);
                        }
                    } else {
                        msg = "illegular format: " + i + "\n";
                        throw kgError(msg);
                    }
                    pivot.axes[1].push_back({vec[0][0], vec[1]});
                }
            }
        }
        if (boost::optional<float> val2 = pt.get_optional<float>("pivot.cutoff")) {
            pivot.cutoff = *val2;
        } else {
            pivot.cutoff = 0;
        }
    } else {
        cerr << "unknown control request" << endl;
        string msg = "unknown control request\n";
        throw kgError(msg);
    }
}

void kgmod::Request::evalRequestFlat(string& req_msg) {
    setQueryDefault();
    string line;
    cerr << "request: \n\"" << req_msg << "\"" << endl;
    stringstream ss(req_msg);
    int c = 0;
    while (getline(ss, line)) {
        Cmn::chomp(line);
        vector<string> vec = Cmn::CsvStr::Parse(line);
        if (vec.size() != 0) {
            etcRec.args.reserve(vec.size() - 1);
            for (size_t i = 0; i < vec.size(); i++) {
                if (i == 0) etcRec.func = vec[i];
                else etcRec.args.push_back(vec[i]);
            }
            if (boost::iequals(line, "bye")) {
                mode = "control";
                return;
            } else if (boost::iequals(vec[0], "ListTraAtt")) {
                mode = "retrieve";
                return;
            } else if (vec.size() != 0 && boost::iequals(vec[0], "GetTraAtt")) {
                mode = "retrieve";
                return;
            }
        }
        
        try {
            mode = "query";
            if (c == 0) {
                query.traFilter = _filter->makeTraBitmap(line);
            } else if (c == 1) {
                query.itemFilter = _filter->makeItemBitmap(line);
            } else if (c == 2) {
                for (size_t i = vec.size(); i < 5; i++) {
                    vec.resize(i + 1);
                    if (i == 4) vec[i] = "-1";
                    else vec[i] = "0";
                }
                query.selCond = {stod(vec[0]), stod(vec[1]), stod(vec[2]), stod(vec[3]), stod(vec[4])};
            } else if (c == 3) {
                // sortKey
                if (boost::iequals(vec[0], "SUP")) {
                    query.sortKey = SORT_SUP;
                } else if (boost::iequals(vec[0], "CONF")) {
                    query.sortKey = SORT_CONF;
                } else if (boost::iequals(vec[0], "LIFT")) {
                    query.sortKey = SORT_LIFT;
                } else if (boost::iequals(vec[0], "JAC")) {
                    query.sortKey = SORT_JAC;
                } else if (boost::iequals(vec[0], "PMI")) {
                    query.sortKey = SORT_PMI;
                } else {
                    query.sortKey = SORT_NONE;
                }
                
                // sendMax
                if (vec.size() == 2) query.sendMax = stoi(vec[1]);
            } else if (c == 4) {
                if (vec.size() >= 1) {
                    boost::trim(vec[0]);
                    if (vec[0] != "") {
                        if (! Cmn::posInVector(_config->traAttFile.granuFields, vec[0])) {
                            string msg = "query.granularity.transaction(";
                            msg += vec[0];
                            msg += ") must be set in config file (traAttFile.granuFields)\n";
                            throw kgError(msg);
                        }
                        query.granularity.first[0] = vec[0];
                    }
                }
                if (vec.size() >= 2) {
                    boost::trim(vec[1]);
                    if (vec[1] != "") query.granularity.second[0] = vec[1];
                }
            } else if (c == 5) {
                query.dimension = makeDimBitmap(line);
            } else if (c == 6) {
                // デバッグ用
                if (boost::iequals(line, "notExec")) {
                    query.debug_mode = 1;
                }
            } else {
                break;
            }
            c++;
        } catch(kgError& err){
            auto msg = err.message();
            string body = "status:-1\n";
            for (auto i = msg.begin(); i != msg.end(); i++) {
                body += *i + "\n";
            }
            throw kgError(body);
        }
    }
}

void kgmod::Request::evalRequest(string& req_msg) {
    mode = "";
    deadlineTimer = _config->deadlineTimer;   // default
    if (req_msg[0] != '{') {
        evalRequestFlat(req_msg);
    } else {
        evalRequestJson(req_msg);
    }
    dump();
}
