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
#include <kgError.h>
#include <kgMethod.h>
#include <kgConfig.h>
#include <kgMessage.h>
#include <chrono>
#include <cfloat>
#include <set>
#include <stack>
#include <boost/algorithm/string.hpp>
#include <boost/functional/hash.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include "kgCSV.h"
#include "bidx-ewah.hpp"
#include "cmn.hpp"
#include "config.hpp"
#include "param.hpp"
#include "cmdcache.hpp"
#include "filter.hpp"
#include "http.hpp"
#include "kggolap.hpp"

using namespace std;
using namespace kgmod;

// -----------------------------------------------------------------------------
// コンストラクタ(モジュール名，バージョン登録,パラメータ)
// -----------------------------------------------------------------------------
kgmod::kgGolap::kgGolap(void)
{
    _name    = "golap";
    _version = "###VERSION###";
    
#include <help/en/kggolapHelp.h>
    _titleL = _title;
    _docL   = _doc;
#ifdef JPN_FORMAT
#include <help/jp/kggolapHelp.h>
#endif
}

kgmod::kgGolap::~kgGolap(void) {
    if (occ != NULL) delete occ;
    if (config != NULL) delete config;
};


// -----------------------------------------------------------------------------
// 引数の設定
// -----------------------------------------------------------------------------
void kgmod::kgGolap::setArgs(void) {
    _args.paramcheck("i=,-d",kgArgs::COMMON|kgArgs::IODIFF|kgArgs::NULL_IN);
    
    opt_inf = _args.toString("i=", false);
    opt_debug = _args.toBool("-d");
}

Dimension kgmod::kgGolap::makeDimBitmap(string& cmdline) {
    boost::trim(cmdline);
    Dimension out;
    if (cmdline == "") {
        out.key = "";
        return out;
    }
    vector<string> param = Cmn::CsvStr::Parse(cmdline);
    out.key = param[0];
    for (size_t i = 1; i < param.size(); i++) {
        out.DimBmpList[param[i]] =  occ->bmpList.GetVal(out.key, param[i]);
    }
    return out;
}

Result kgmod::Enum(Query& query, Ewah& dimBmp) {
    const string csvHeader = "node1,node2,frequency,frequency1,frequency2,total,support,confidence,lift,jaccard,PMI,node1n,node2n";
    cerr << "filtering" << endl;
    size_t hit = 0;
    bool notSort = false;
    signed int stat = 0;
    Result res;
    unordered_map<size_t, size_t> itemFreq;
    
    Ewah tarTraBmp = query.traFilter & dimBmp;
    size_t traNum = tarTraBmp.numberOfOnes();
    if (traNum == 0) {
        cerr << "trabitmap is empty" << endl;
        res.insert(make_pair(0, "status:0,sent:0,hit:0"));
        res.insert(make_pair(1, csvHeader));
        return res;
    }
    
    // traUniqAttKeyを指定していた場合は、traNumをtraUniqAttKeyのデータ数で上書き
    if (query.granularity.first != mt_config->traFile.traFld) {
        cerr << "count key value" << endl;
        traNum = mt_occ->countKeyValue(query.granularity.first, &query.traFilter);
    }

    unordered_map<string, bool> checked_node2;  // [node] -> exists
    vector<string> tra2key;
    if (query.granularity.first != mt_config->traFile.traFld) {
        mt_occ->getTra2KeyValue(query.granularity.first, &tra2key);
    }
    for (auto i2 = query.itemFilter.begin(), ei2 = query.itemFilter.end(); i2 != ei2; i2++) {
        if (isTimeOut) {stat = 2; break;}
        string node2;
        if (query.granularity.second == mt_config->traFile.itemFld) {
            node2 = mt_occ->itemAtt->item[(*i2)];
        } else {
            node2 = mt_occ->itemAtt->key2att(*i2, query.granularity.second);
            if (checked_node2.find(node2) == checked_node2.end()) {
                checked_node2[node2] = true;
            } else {
                continue;
            }
        }
        
        map<size_t, size_t> coitems;
        unordered_map<pair<string, size_t>, bool, boost::hash<pair<string, size_t>>> checked_node1;
            // [node, traNo] -> exists
        unordered_map<pair<size_t, size_t>, bool, boost::hash<pair<size_t, size_t>>> checked_item1;
            // [itemNo, traNo] -> exists
        unordered_map<string, size_t> itemNo4node_map;    // [node] -> coitemsをカウントする代表itemNo
        Ewah itemInTheAtt2 = mt_occ->itemAtt->bmpList.GetVal(query.granularity.second, node2);
        itemInTheAtt2 = itemInTheAtt2 & query.itemFilter;
        for (auto at2 = itemInTheAtt2.begin(), eat2 = itemInTheAtt2.end(); at2 != eat2; at2++) {
            Ewah* tra_i2_tmp;
            mt_occ->bmpList.GetVal(mt_occ->occKey, mt_occ->itemAtt->item[*at2], tra_i2_tmp);
            Ewah tra_i2 = *tra_i2_tmp & tarTraBmp;
            
            unordered_map<pair<string, size_t>, bool, boost::hash<pair<string, size_t>>> checked_traAttVal;
            for (auto t2 = tra_i2.begin(), et2 = tra_i2.end(); t2 != et2; t2++) {
                if (isTimeOut) {stat = 2; break;}
                string& traAttVal = tra2key[*t2];
                Ewah item_i1 = mt_occ->occ[*t2] & query.itemFilter;
                for (auto i1 = item_i1.begin(), ei1 = item_i1.end(); i1 != ei1; i1++) {
                    if (*i1 >= *i2) break;
                    
                    string node1;
                    if (query.granularity.second == mt_config->traFile.itemFld) {
                        node1 = mt_occ->itemAtt->item[*i1];
                    } else {
                        node1 = mt_occ->itemAtt->key2att(*i1, query.granularity.second);
                        if (checked_node1.find({node1, *t2}) == checked_node1.end()) {
                            checked_node1[{node1, *t2}] = true;
                        } else {
                            continue;
                        }
                    }
                    
                    Ewah itemInTheAtt1 = mt_occ->itemAtt->bmpList.GetVal(query.granularity.second, node1);
                    itemInTheAtt1 = itemInTheAtt1 & query.itemFilter;
                    for (auto at1 = itemInTheAtt1.begin(), eat1 = itemInTheAtt1.end(); at1 != eat1; at1++) {
                        size_t itemNo4node;
                        if (query.granularity.second == mt_config->traFile.itemFld) {
                            itemNo4node = *i1;
                        } else {
                            if (itemNo4node_map.find(node1) == itemNo4node_map.end()) {
                                itemNo4node_map[node1] = *i1;
                                itemNo4node = *i1;
                            } else {
                                itemNo4node = itemNo4node_map[node1];
                            }
                            //
                            if (checked_item1.find({itemNo4node, *t2}) == checked_item1.end()) {
                                checked_item1[{itemNo4node, *t2}] = true;
                            } else {
                                continue;
                            }
                        }
                        
                        if (query.granularity.first == mt_config->traFile.traFld) {
                            coitems[itemNo4node]++;
                        } else {
                            if (checked_traAttVal.find({traAttVal, *i1}) == checked_traAttVal.end()) {
                                coitems[itemNo4node]++;
                                checked_traAttVal[{traAttVal, *i1}] = true;
                            }
                        }
                    }
                }
            }
        }
        
        for (auto c = coitems.begin(), ec = coitems.end(); c != ec; c++) {
            if (isTimeOut) {stat = 2; break;}
            string item1, item2;
            string itemName1, itemName2;
            if (query.granularity.second == mt_config->traFile.itemFld) {
                item1 = mt_occ->itemAtt->item[c->first];
                item2 = mt_occ->itemAtt->item[*i2];
                itemName1 = mt_occ->itemAtt->itemName[c->first];
                itemName2 = mt_occ->itemAtt->itemName[*i2];
            } else {
                item1 = mt_occ->itemAtt->key2att(c->first, query.granularity.second);
                item2 = mt_occ->itemAtt->key2att(*i2, query.granularity.second);
                itemName1 = mt_occ->itemAtt->code2name(query.granularity.second, item1);;
                itemName2 = mt_occ->itemAtt->code2name(query.granularity.second, item2);
            }
            
            size_t freq = c->second;
            float sup = (float)freq / traNum;
            if (sup < query.selCond.minSup) continue;
            
            if (query.granularity.second == mt_config->traFile.itemFld) {
                if (query.granularity.first == mt_config->traFile.traFld) {
                    if (itemFreq.find(c->first) == itemFreq.end())
                        itemFreq[c->first] = mt_occ->itemFreq(c->first, tarTraBmp);
                    if (itemFreq.find(*i2) == itemFreq.end())
                        itemFreq[*i2] = mt_occ->itemFreq(*i2, tarTraBmp);
                } else {
                    if (itemFreq.find(c->first) == itemFreq.end())
                        itemFreq[c->first] = mt_occ->itemFreq(c->first, tarTraBmp, &tra2key);
                    if (itemFreq.find(*i2) == itemFreq.end())
                        itemFreq[*i2] = mt_occ->itemFreq(*i2, tarTraBmp, &tra2key);
                }
            } else {
                string attVal1 = mt_occ->itemAtt->key2att(c->first, query.granularity.second);
                string attVal2 = mt_occ->itemAtt->key2att(*i2, query.granularity.second);
                if (query.granularity.first == mt_config->traFile.traFld) {
                    if (itemFreq.find(c->first) == itemFreq.end())
                        itemFreq[c->first] = mt_occ->attFreq(query.granularity.second, attVal1, tarTraBmp);
                    if (itemFreq.find(*i2) == itemFreq.end())
                        itemFreq[*i2] = mt_occ->attFreq(query.granularity.second, attVal2, tarTraBmp);
                } else {
                    if (itemFreq.find(c->first) == itemFreq.end())
                        itemFreq[c->first] = mt_occ->attFreq(query.granularity.second, attVal1, tarTraBmp, &tra2key);
                    if (itemFreq.find(*i2) == itemFreq.end())
                        itemFreq[*i2] = mt_occ->attFreq(query.granularity.second, attVal2, &tra2key);
                }
            }
            float conf1 = (float)freq / itemFreq[*i2];
            if (conf1 < query.selCond.minConf) continue;
//            float conf2 = (float)freq / itemFreq[c->first];
            
            float jac = (float)freq / (itemFreq[*i2] + itemFreq[c->first] - freq);
            if (jac < query.selCond.minJac) continue;
            float lift = (float)(freq * traNum) / (itemFreq[*i2] * itemFreq[c->first]);
            if (lift < query.selCond.minLift) continue;
            
            float pmi = Cmn::calcPmi(freq, itemFreq[*i2], itemFreq[c->first], traNum);
            if (pmi < query.selCond.minPMI) continue;
            
            char msg[1024];
            // node1,node2,frequency,frequency1,frequency2,total,support,confidence,lift,jaccard,PMI,node1n,node2n
            const char* fmt = "%s,%s,%d,%d,%d,%d,%f,%f,%f,%f,%f,%s,%s";
            sprintf(msg, fmt, item1.c_str(), item2.c_str(), freq, itemFreq[*i2], itemFreq[c->first], traNum,
                    sup, conf1, lift, jac, pmi, itemName1.c_str(), itemName2.c_str());
            float skey;
            if (query.sortKey == SORT_SUP) {
                skey = -sup;
            } else if (query.sortKey == SORT_CONF) {
//                skey = -min(conf1,conf2);
                skey = -conf1;
            } else if (query.sortKey == SORT_LIFT) {
                skey = -lift;
            } else if (query.sortKey == SORT_JAC) {
                skey = -jac;
            } else if (query.sortKey == SORT_PMI) {
                skey = -pmi;
            } else {
                notSort = true;
                skey = -10;
            }
            res.insert(make_pair(skey, string(msg)));
            
            if (res.size() > query.sendMax) {
                auto pos = res.end();
                pos--;
                res.erase(pos);
                stat = 1;
                if (notSort) break;
            }
            hit++;
        }
    }
    
    string buf = "status:" + to_string(stat);
    buf += ",sent:" + to_string(res.size());
    buf += ",hit:" + to_string(hit);
    cerr << buf << endl;
    buf += "\n";
    buf += csvHeader;
    res.insert(make_pair(-FLT_MAX, buf));
    return res;
}

void kgmod::MT_Enum(mq_t* mq, Query* query, map<string, Result>* res) {
    mq_t::th_t* T = mq->pop();
    while (T != NULL) {
        cerr << "#" << T->first << ") tarTra:"; Cmn::CheckEwah(T->second.second);
        Result rr = Enum(*query, *(T->second.second));
        (*res)[T->second.first] = rr;
        delete T->second.second;
        delete T;
        T = mq->pop();
    }
}

void kgmod::exec::co_occurrence(Query& query, map<string, Result>& res) {
    if (query.dimension.DimBmpList.size() == 0) {
        Ewah allTraBmp;
        allTraBmp.padWithZeroes(golap_->occ->traAtt->traMax);
        allTraBmp.inplace_logicalnot();
        res[""] = kgmod::Enum(query, allTraBmp);
    } else {
        mq_t::th_t *th;
        if (mt_config->mt_enable) {
            mq_t mq;
            size_t threadNo = 0;
            for (auto i = query.dimension.DimBmpList.begin(); i != query.dimension.DimBmpList.end(); i++) {
                cerr << "running with multi-threading (";
                cerr << threadNo << " of " << query.dimension.DimBmpList.size() << ")" << endl;
                th = new mq_t::th_t;
                th->first = threadNo;
                th->second.first = i->first;
                th->second.second = new Ewah;
                th->second.second->expensive_copy(i->second);
                mq.push(th);
                threadNo++;
            }
            
            vector<boost::thread> thg;
            for (int i = 0; i < mt_config->mt_degree; i++) {
                thg.push_back(boost::thread([&mq, &query, &res] {
                    MT_Enum(&mq, &query, &res);
                }));
            }
            
            for (boost::thread& th : thg) {
                th.join();
            }
        } else {
            for (auto i = query.dimension.DimBmpList.begin(); i != query.dimension.DimBmpList.end(); i++) {
                res[i->first] = kgmod::Enum(query, i->second);
            }
        }
    }
}

void kgmod::exec::axisValsList(vector<pair<char, string>>& flds, vector<vector<string>>& valsList) {
    valsList.resize(flds.size());
    for (size_t i = 0; i < flds.size(); i++) {
        vector<string> val;
        if (flds[i].first == 'T') {
            val = mt_occ->bmpList.EvalKeyValue(flds[i].second);
        } else if (flds[i].first == 'I') {
            val = mt_occ->itemAtt->bmpList.EvalKeyValue(flds[i].second);
        }
        valsList[i] = val;
    }
}

void kgmod::exec::item2traBmp(string itemKey, string itemVal, Ewah& traBmp) {
    traBmp.reset();
    Ewah itemBmp = mt_occ->itemAtt->bmpList.GetVal(itemKey, itemVal);
    for (auto i = itemBmp.begin(), ie = itemBmp.end(); i != ie; i++) {
        string tar = mt_occ->itemAtt->item[*i];
        Ewah tmp = mt_occ->bmpList.GetVal(mt_occ->occKey, tar);
        traBmp = traBmp | tmp;
    }
}

void kgmod::exec::pivot(Pivot& pivot, map<string, Result>& res) {
    pair<map<pair<string, string>, Ewah>, map<pair<string, string>, Ewah>> bmp;
    pair<vector<vector<string>>, vector<vector<string>>> valsList;
    axisValsList(pivot.axes.first, valsList.first);
    size_t i = 0;
    for (auto& vals : valsList.first) {
        Ewah itemBmp;
        for (auto& val : vals) {
            if (pivot.axes.first[i].first == 'I') {
                if (pivot.axes.first[i].first == 'T') {
                    bmp.first[{pivot.axes.first[i].second, val}] = mt_occ->bmpList.GetVal(pivot.axes.first[i].second, val);
                } else if (pivot.axes.first[i].first == 'I') {
                    item2traBmp(pivot.axes.first[i].second, val, bmp.first[{pivot.axes.first[i].second, val}]);
                }
            }
            i++;
        }
    }
    axisValsList(pivot.axes.second, valsList.second);
    i = 0;
    for (auto& vals : valsList.second) {
        size_t v = 0;
        Ewah itemBmp;
        for (auto& val : vals) {
            if (pivot.axes.second[i].first == 'T') {
                bmp.first[{pivot.axes.second[i].second, val}] = mt_occ->bmpList.GetVal(pivot.axes.second[i].second, val);
            } else if (pivot.axes.second[i].first == 'I') {
                item2traBmp(pivot.axes.second[i].second, val, bmp.first[{pivot.axes.second[i].second, val}]);
            }
            v++;
        }
        i++;
    }
    
    map<pair<vector<string>, vector<string>>, size_t> mat;
    for (auto& x : bmp.first) {
        for (auto& y : bmp.second) {
            Ewah cross = x.second & y.second;
            size_t num = cross.numberOfOnes();
/////            if (num != 0) mat[{x.first, y.first}] = num;
        }
    }

    pair<map<vector<string>, bool>, map<vector<string>, bool>> axesHeader;  // first:x-axis, second:y-axies
    for (auto i = mat.begin(), ei = mat.end(); i != ei; i++) {
        axesHeader.first[i->first.first] = true;
        axesHeader.second[i->first.second] = true;
    }
    
    float lnum = 0;
    for (auto& y_hdr : axesHeader.second) {
        string line;
        for (auto& y_hfld : y_hdr.first) {
            line += y_hfld; line += ",";
        }
        for (auto& x_hdr : axesHeader.first) {
            size_t cnt;
            if (mat.find({x_hdr.first, y_hdr.first}) == mat.end()) {
                cnt = 0;
            } else {
                cnt = mat[{x_hdr.first, y_hdr.first}];
            }
            line += to_string(cnt);
        }
        res[""].insert(make_pair(lnum, line));
    }
}

void kgmod::exec::setQueryDefault(Query& query) {
    query.traFilter.padWithZeroes(golap_->occ->traAtt->traMax);
    query.traFilter.inplace_logicalnot();
    query.itemFilter.padWithZeroes(golap_->occ->itemAtt->itemMax);
    query.itemFilter.inplace_logicalnot();
    query.selCond = {0, 0, 0, 0, -1};
    query.debug_mode = 0;
    query.granularity.first  = mt_config->traFile.traFld;
    query.granularity.second = mt_config->traFile.itemFld;
    query.sendMax = mt_config->sendMax;
}

bool kgmod::exec::evalRequestJson(Request& request) {
    cerr << "request: " << req_body() << endl;
    bool stat = true;
    string res_body;
    try {
        stringstream json(req_body());
        boost::property_tree::ptree pt;
        boost::property_tree::read_json(json, pt);
//        cerr << "request: " << endl;
//        boost::property_tree::write_json(cerr, pt, true);
        
        if (boost::optional<unsigned int> val = pt.get_optional<unsigned int>("deadlineTimer")) {
            request.deadlineTimer = *val;
        } else {
            request.deadlineTimer = mt_config->deadlineTimer;
        }
        
        if (boost::optional<string> val = pt.get_optional<string>("control")) {
            if (boost::iequals(*val, "bye")) {
                cerr << "bye message recieved" << endl;
                closing_ = true;
                res_body = "terminated\n";
            } else {
                cerr << "unknown control request" << endl;
                res_body = "unknown control request\n";
            }
            stat = false;
        } else if (boost::optional<string> val = pt.get_optional<string>("retrieve")) {
            vector<string> vec = Cmn::CsvStr::Parse(*val);
            if (boost::iequals(vec[0], "ListTraAtt")) {
                cerr << vec[0] << endl;
                res_body ="TraAtt\n";
                vector<string> traAtts = mt_occ->traAtt->listAtt();
                for (auto att = traAtts.begin(); att != traAtts.end(); att++) {
                    res_body += *att; res_body += "\n";
                }
    //        } else if (boost::iequals(vec[0], "ListItemAtt")) {
            } else if (boost::iequals(vec[0], "GetTraAtt")) {
                cerr << vec[0] << endl;
                res_body = vec[1];  res_body += "\n";
                vector<string> attVal = mt_occ->evalKeyValue(vec[1]);
                for (auto att = attVal.begin(); att != attVal.end(); att++) {
                    res_body += *att; res_body += "\n";
                }
    //        } else if (boost::iequals(vec[0], "GetItemAtt")) {
            } else {
                cerr << "unknown control request" << endl;
                res_body = "unknown control request\n";
            }
            stat = false;
        } else if (boost::optional<string> val = pt.get_optional<string>("query")) {
            request.mode = "query";
            setQueryDefault(request.query);
            if (boost::optional<string> val2 = pt.get_optional<string>("query.traFilter")) {
                request.query.traFilter = golap_->fil->makeTraBitmap(*val2);
            }
            if (boost::optional<string> val2 = pt.get_optional<string>("query.itemFilter")) {
                request.query.itemFilter = golap_->fil->makeItemBitmap(*val2);
            }
            if (boost::optional<string> val2 = pt.get_optional<string>("query.selCond")) {
                if (boost::optional<double> val3 = pt.get_optional<double>("query.selCond.minSup")) {
                    request.query.selCond.minSup = *val3;
                }
                if (boost::optional<double> val3 = pt.get_optional<double>("query.selCond.minConf")) {
                    request.query.selCond.minConf = *val3;
                }
                if (boost::optional<double> val3 = pt.get_optional<double>("query.selCond.minLift")) {
                    request.query.selCond.minLift = *val3;
                }
                if (boost::optional<double> val3 = pt.get_optional<double>("query.selCond.minJac")) {
                    request.query.selCond.minJac = *val3;
                }
                if (boost::optional<double> val3 = pt.get_optional<double>("query.selCond.minPMI")) {
                    request.query.selCond.minPMI = *val3;
                }
                request.query.selCond.dump();
            }
            if (boost::optional<string> val2 = pt.get_optional<string>("query.sortKey")) {
                if (boost::iequals(*val2, "SUP")) {
                    request.query.sortKey = SORT_SUP;
                } else if (boost::iequals(*val2, "CONF")) {
                    request.query.sortKey = SORT_CONF;
                } else if (boost::iequals(*val2, "LIFT")) {
                    request.query.sortKey = SORT_LIFT;
                } else if (boost::iequals(*val2, "JAC")) {
                    request.query.sortKey = SORT_JAC;
                } else if (boost::iequals(*val2, "PMI")) {
                    request.query.sortKey = SORT_PMI;
                } else {
                    request.query.sortKey = SORT_NONE;
                }
//                cerr << "sortKey: " << *val2 << endl;
            }
            if (boost::optional<string> val2 = pt.get_optional<string>("query.sendMax")) {
                request.query.sendMax = stoi(*val2);
//                cerr << "sendMax: " << request.query.sendMax << endl;
            }
            if (boost::optional<string> val2 = pt.get_optional<string>("query.granularity")) {
                if (boost::optional<string> val3 = pt.get_optional<string>("query.granularity.transaction")) {
                    request.query.granularity.first = *val3;
                }
                if (boost::optional<string> val3 = pt.get_optional<string>("query.granularity.node")) {
                    request.query.granularity.second = *val3;
                }
            }
            if (boost::optional<string> val2 = pt.get_optional<string>("query.dimension")) {
                request.query.dimension = golap_->makeDimBitmap(*val2);
            }
            if (boost::optional<string> val2 = pt.get_optional<string>("query.debug_mode")) {
                // デバッグ用
                if (boost::iequals(*val2, "notExec")) {
                    request.query.debug_mode = 1;
                    string body = *val2;
                    stat = false;
                }
            }
        } else if (boost::optional<string> val = pt.get_optional<string>("pivot")) {
            request.mode = "pivot";
            if (boost::optional<string> val2 = pt.get_optional<string>("pivot.traFilter")) {
                request.pivot.traFilter = golap_->fil->makeTraBitmap(*val2);
            }
            if (boost::optional<string> val2 = pt.get_optional<string>("pivot.itemFilter")) {
                request.pivot.itemFilter = golap_->fil->makeItemBitmap(*val2);
            }
            if (boost::optional<string> val2 = pt.get_optional<string>("pivot.x-axis")) {
                vector<string>csv = Cmn::CsvStr::Parse(*val2);
                request.pivot.axes.first.reserve(csv.size());
                for (auto& i : csv) {
                    vector<string>vec = Cmn::Split(i, ':');
                    transform(vec[0].cbegin(), vec[0].cend(), vec[0].begin(), ::toupper);
                    request.pivot.axes.first.push_back({vec[0][0], vec[1]});
                }
            }
            if (boost::optional<string> val2 = pt.get_optional<string>("pivot.y-axis")) {
                vector<string>csv = Cmn::CsvStr::Parse(*val2);
                request.pivot.axes.second.reserve(csv.size());
                for (auto& i : csv) {
                    vector<string>vec = Cmn::Split(i, ':');
                    transform(vec[0].cbegin(), vec[0].cend(), vec[0].begin(), ::toupper);
                    request.pivot.axes.second.push_back({vec[0][0], vec[1]});
                }
            }
        } else {
            cerr << "unknown control request" << endl;
            res_body = "unknown control request\n";
            stat = false;
        }
    } catch (boost::property_tree::json_parser_error& e) {
        res_body = "#ERROR# "; res_body += e.what();
        cerr << res_body << endl;
        stat = false;
    }
    
    if (! stat) {
        put_send_data(res_body);
        Http::proc();
    }
    return stat;
}

bool kgmod::exec::evalRequestFlat(Request& request) {
    setQueryDefault(request.query);
    string line;
    cerr << "request: \n\"" << req_body() << "\"" << endl;
    stringstream ss(req_body());
    int c = 0;
    while (getline(ss, line)) {
        Cmn::chomp(line);
        vector<string> vec = Cmn::CsvStr::Parse(line);
        if (boost::iequals(line, "bye")) {
            cerr << "bye message recieved" << endl;
            closing_ = true;
            string res_body = "terminated\n";
            put_send_data(res_body);
            Http::proc();
            return false;
        } else if (boost::iequals(line, "ListTraAtt")) {
            cerr << vec[0] << endl;
            string res_body ="TraAtt\n";
            vector<string> traAtts = mt_occ->traAtt->listAtt();
            for (auto att = traAtts.begin(); att != traAtts.end(); att++) {
                res_body += *att; res_body += "\n";
            }
            put_send_data(res_body);
            Http::proc();
            return false;
        } else if (vec.size() != 0 && boost::iequals(vec[0], "GetTraAtt")) {
            cerr << vec[0] << endl;
            string res_body = vec[1];  res_body += "\n";
            vector<string> attVal = mt_occ->evalKeyValue(vec[1]);
            for (auto att = attVal.begin(); att != attVal.end(); att++) {
                res_body += *att; res_body += "\n";
            }
            put_send_data(res_body);
            Http::proc();
            return false;
        }
        
        try {
            request.mode = "query";
            if (c == 0) {
                request.query.traFilter = golap_->fil->makeTraBitmap(line);
            } else if (c == 1) {
                request.query.itemFilter = golap_->fil->makeItemBitmap(line);
            } else if (c == 2) {
                for (size_t i = vec.size(); i < 5; i++) {
                    vec.resize(i + 1);
                    if (i == 4) vec[i] = "-1";
                    else vec[i] = "0";
                }
                request.query.selCond = {stod(vec[0]), stod(vec[1]), stod(vec[2]), stod(vec[3]), stod(vec[4])};
            } else if (c == 3) {
                // sortKey
                if (boost::iequals(vec[0], "SUP")) {
                    request.query.sortKey = SORT_SUP;
                } else if (boost::iequals(vec[0], "CONF")) {
                    request.query.sortKey = SORT_CONF;
                } else if (boost::iequals(vec[0], "LIFT")) {
                    request.query.sortKey = SORT_LIFT;
                } else if (boost::iequals(vec[0], "JAC")) {
                    request.query.sortKey = SORT_JAC;
                } else if (boost::iequals(vec[0], "PMI")) {
                    request.query.sortKey = SORT_PMI;
                } else {
                    request.query.sortKey = SORT_NONE;
                }
                
                // sendMax
                if (vec.size() == 2) request.query.sendMax = stoi(vec[1]);
            } else if (c == 4) {
                if (vec.size() >= 1) {
                    boost::trim(vec[0]);
                    if (vec[0] != "") request.query.granularity.first = vec[0];
                }
                if (vec.size() >= 2) {
                    boost::trim(vec[1]);
                    if (vec[1] != "") request.query.granularity.second = vec[1];
                }
            } else if (c == 5) {
                request.query.dimension = golap_->makeDimBitmap(line);
            } else if (c == 6) {
                // デバッグ用
                if (boost::iequals(line, "notExec")) {
                    string body = line;
                    put_send_data(body);
                    Http::proc();
                    return false;
                }
            } else {
                break;
            }
            c++;
        } catch(kgError& err){
            auto msg = err.message();
            for (auto i = msg.begin(); i != msg.end(); i++) {
                string body = *i + "\n";
                put_send_data(body);
                Http::proc();
            }
            return false;
        }
    }
    return true;
}

bool kgmod::exec::evalRequest(Request& request) {
    bool stat;
    string body = req_body();
    request.deadlineTimer = mt_config->deadlineTimer;   // default
    if (body[0] != '{') {
        stat = evalRequestFlat(request);
    } else {
        stat = evalRequestJson(request);
    }
    if (stat) request.dump();
    return stat;
}

void kgmod::exec::proc(void) {
    // Excecuting Enum on each dimention
    chrono::system_clock::time_point Start = chrono::system_clock::now();
    map<string, Result> res;
    
    Request request;
    if (! evalRequest(request)) return;
    
    // 重たい処理の場合、timerによってisTimeOutがfalseからtrueに変えられる
    // 各ファンクション内のループの先頭でisTimeOutをチェックしtreeの場合ループを強制的に抜ける
    setTimer(request.deadlineTimer);
    if (request.mode == "query") {
        co_occurrence(request.query, res);
    } else if (request.mode == "pivot") {
        pivot(request.pivot, res);
    }
    cancelTimer();
    
    chrono::system_clock::time_point End = chrono::system_clock::now();
    double elapsed = chrono::duration_cast<chrono::milliseconds>(End-Start).count();
    cerr << "process time: " << elapsed / 1000 << " sec" <<endl;
    
    cerr << "sending" << endl;
    string body;
    for (auto i = res.begin(); i != res.end(); i++) {
        if (i->first != "") {
            body += request.query.dimension.key; body += ":"; body += i->first; body += "\n";
        }
        for (auto j = i->second.begin(); j != i->second.end(); j++) {
            body += j->second; body += "\n";
        }
        body += "\n";
    }
    put_send_data(body);
    Http::proc();
}

//
int kgmod::kgGolap::run() {
    try {
        setArgs();
        config = new Config(opt_inf);
        config->dump(opt_debug);
        mt_config = config;         // マルチスレッド用反則技
        
        occ = new Occ(config, _env);
        occ->load();
        occ->dump(opt_debug);
        mt_occ = occ;               // マルチスレッド用反則技
        
        cmdcache = new cmdCache(config, _env, false);
        cmdcache->dump(opt_debug);
        fil = new Filter(occ, cmdcache, config, _env, opt_debug);
        
        cerr << "port#: " << config->port << endl;
        while (true) {
            cmdcache->save();
            
            asio::io_service io_service;
            exec server(this, io_service, config->port);
            server.start();
            io_service.run();
            if (server.isClosing()) break;
            server.stop();
            io_service.stop();
        }
        cerr << "terminated" << endl;
        
    } catch(kgError& err){
        errorEnd(err);
        return EXIT_FAILURE;
        
    } catch(char* er){
        kgError err(er);
        errorEnd(err);
        return EXIT_FAILURE;
    }
    
    return EXIT_SUCCESS;
}
