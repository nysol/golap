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
    query.granularity.first  = _config->traFile.traFld;
    query.granularity.second = _config->traFile.itemFld;
    query.dimension.key = "";
    query.dimension.DimBmpList.clear();
    query.sendMax = _config->sendMax;
}

void kgmod::Request::evalRequestJson(string& req_msg) {
    cerr << "request: " << req_msg << endl;
    stringstream json(req_msg);
    boost::property_tree::ptree pt;
    boost::property_tree::read_json(json, pt);
    //    cerr << "request: " << endl;
    //    boost::property_tree::write_json(cerr, pt, true);
    
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
            //            cerr << "sortKey: " << *val2 << endl;
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("query.sendMax")) {
            query.sendMax = stoi(*val2);
            //            cerr << "sendMax: " << request.query.sendMax << endl;
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("query.granularity")) {
            if (boost::optional<string> val3 = pt.get_optional<string>("query.granularity.transaction")) {
                if (Cmn::posInVector(_config->traAttFile.granuFields, *val3)) {
                    query.granularity.first = *val3;
                } else {
                    string msg = "status:-1\n";
                    msg += "query.granularity.transaction(";
                    msg += *val3;
                    msg += ") must be set in config file (traAttFile.granuFields)\n";
                    throw msg;
                }
            }
            if (boost::optional<string> val3 = pt.get_optional<string>("query.granularity.node")) {
                query.granularity.second = *val3;
            }
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("query.dimension")) {
            query.dimension = makeDimBitmap(*val2);
        }
        if (boost::optional<string> val2 = pt.get_optional<string>("query.debugMode")) {
            // デバッグ用
            if (boost::iequals(*val2, "notExec")) {
                query.debug_mode = 1;
                string msg = "status:-1\n" + *val2 + "\n";
                throw msg;
            } else if (boost::iequals(*val2, "checkData")) {
                query.debug_mode = 2;
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
            string tmp = "sel_item(" + *val2 + ")";
            Ewah bmp = _filter->makeTraBitmap(tmp);
            pivot.traFilter = pivot.traFilter & bmp;
        }
        pivot.axes.resize(2);
        if (boost::optional<string> val2 = pt.get_optional<string>("pivot.colAtt")) {
            vector<string>csv = Cmn::CsvStr::Parse(*val2);
            pivot.axes[0].reserve(csv.size());
            for (auto& i : csv) {
                vector<string>vec = Cmn::Split(i, ':');
                if (vec.size() == 1) {
                    if ((! Cmn::posInVector(_config->traAttFile.strFields, vec[0])) &&
                        (! Cmn::posInVector(_config->traAttFile.catFields, vec[0]))) {
                        string msg = "status:-1\n" + vec[0] + " is not transaction attribute\n";
                        throw msg;
                    }
                    pivot.axes[0].push_back({'T', vec[0]});
                } else if (vec.size() >= 2) {
                    transform(vec[0].cbegin(), vec[0].cend(), vec[0].begin(), ::toupper);
                    string msg;
                    if (vec[0][0] == 'T') {
                        if ((! Cmn::posInVector(_config->traAttFile.strFields, vec[1])) &&
                            (! Cmn::posInVector(_config->traAttFile.catFields, vec[1]))) {
                            msg = "status:-1\n" + vec[1] + " is not transaction attribute\n";
                            throw msg;
                        }
                    } else if (vec[0][0] == 'I') {
                        if (! Cmn::posInVector(_config->itemAttFile.strFields, vec[1])) {
                            msg = "status:-1\n" + vec[1] + " is not transaction attribute\n";
                            throw msg;
                        }
                    } else {
                        msg = "status:-1\nillegular format: " + i + "\n";
                        throw msg;
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
                    if ((! Cmn::posInVector(_config->traAttFile.strFields, vec[0])) &&
                        (! Cmn::posInVector(_config->traAttFile.catFields, vec[0]))) {
                        string msg = "status:-1\n" + vec[0] + " is not transaction attribute\n";
                        throw msg;
                    }
                    pivot.axes[1].push_back({'T', vec[0]});
                } else if (vec.size() >= 2) {
                    transform(vec[0].cbegin(), vec[0].cend(), vec[0].begin(), ::toupper);
                    string msg;
                    if (vec[0][0] == 'T') {
                        if ((! Cmn::posInVector(_config->traAttFile.strFields, vec[1])) &&
                            (! Cmn::posInVector(_config->traAttFile.catFields, vec[1]))) {
                            msg = "status:-1\n" + vec[1] + " is not transaction attribute\n";
                            throw msg;
                        }
                    } else if (vec[0][0] == 'I') {
                        if (! Cmn::posInVector(_config->itemAttFile.strFields, vec[1])) {
                            msg = "status:-1\n" + vec[1] + " is not transaction attribute\n";
                            throw msg;
                        }
                    } else {
                        msg = "status:-1\nillegular format: " + i + "\n";
                        throw msg;
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
        string msg = "status:-1\nunknown control request\n";
        throw msg;
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
                            string msg = "status:-1\nquery.granularity.transaction(";
                            msg += vec[0];
                            msg += ") must be set in config file (traAttFile.granuFields)\n";
                            throw msg;
                        }
                        query.granularity.first = vec[0];
                    }
                }
                if (vec.size() >= 2) {
                    boost::trim(vec[1]);
                    if (vec[1] != "") query.granularity.second = vec[1];
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
            throw body;
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
