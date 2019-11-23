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
#include <cstdlib>
#include <fstream>
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

Result kgmod::Enum(Query& query, Ewah& dimBmp) {
    const string csvHeader = "node1,node2,frequency,frequency1,frequency2,total,support,confidence,lift,jaccard,PMI,node1n,node2n";
    cerr << "enumerating" << endl;
    size_t hit = 0;
    bool notSort = false;
    signed int stat = 0;
    Result res;
    unordered_map<size_t, size_t> itemFreq;
    
    Ewah tarTraBmp = query.traFilter & dimBmp & mt_occ->liveTra;
    size_t traNum = tarTraBmp.numberOfOnes();
    if (traNum == 0) {
        cerr << "trabitmap is empty" << endl;
        res.insert(make_pair(0, "status:0,sent:0,hit:0"));
        res.insert(make_pair(1, csvHeader));
        return res;
    }
    
    // granularityのsizeが1で、かつ、[0]がキーであれば、granularityを指定していない(=false)
    bool isTraGranu  = (query.granularity.first.size() != 1) ||
                        (query.granularity.first[0] != mt_config->traFile.traFld);
    bool isNodeGranu = (query.granularity.second.size() != 1) ||
                        (query.granularity.second[0] != mt_config->traFile.itemFld);
    vector<string> tra2key;     // [traNo] -> 当該トランザクションが有するTraAttの値リスト(csv)
    // transaction granularityを指定していた場合は、traNumを指定したtraAttの集計値で上書きする
    if (isTraGranu) {
        // vector版countKeyValueは処理が比較的重いので、マルチバリューかどうかで処理を分ける
        if (query.granularity.first.size() == 1) {
            traNum = mt_occ->countKeyValue(query.granularity.first[0], &tarTraBmp);
            mt_occ->getTra2KeyValue(query.granularity.first[0], tra2key);
        } else {
            traNum = mt_occ->countKeyValue(query.granularity.first, tarTraBmp);
            mt_occ->getTra2KeyValue(query.granularity.first, tra2key);
        }
    }
    
    unordered_map<string, Ewah> ex_occ_cacheOnceQuery;      // ["field name"] -> item bitmap
    unordered_map<vector<string>, bool, boost::hash<vector<string>>> checked_node2;  // [vnodes] -> exists
    for (auto i2 = query.itemFilter.begin(), ei2 = query.itemFilter.end(); i2 != ei2; i2++) {
        if (isTimeOut) {stat = 2; break;}
        vector<string> vnode2;
        string node2;
        if (isNodeGranu) {
            // itemNo(2)から指定した粒度のキー(node2)を作成する。
            vnode2 = mt_occ->itemAtt->key2att(*i2, query.granularity.second);
            node2 = Cmn::CsvStr::Join(vnode2, ":");
        } else {
            node2 = mt_occ->itemAtt->item[(*i2)];
            vnode2.resize(1);
            vnode2[0] = node2;
        }
        
        bool br = false;
        if (node2 == "30/-天竺 半袖Tシャツ_キッズ女児") {
            br = true;
        }
        
        if (itemFreq.find(*i2) == itemFreq.end()) {
            if (isTraGranu) {
                itemFreq[*i2] = mt_occ->attFreq(query.granularity.second, vnode2,
                                                tarTraBmp, query.itemFilter, &tra2key);
            } else {
                itemFreq[*i2] = mt_occ->attFreq(query.granularity.second, vnode2,
                                                tarTraBmp, query.itemFilter);
            }
        }
        if (itemFreq[*i2] == 0) continue;
        
        // すでに処理済みのキーの場合はcontinueする。
        if (checked_node2.find(vnode2) == checked_node2.end()) {
            checked_node2[vnode2] = true;
        } else {
            continue;
        }
        
        map<size_t, size_t> coitems;
        unordered_map<pair<string, string>, bool, boost::hash<pair<string, string>>> checked_node1;
                                                            // [vnodes, traAtt(:区切り)] -> exists
        unordered_map<pair<size_t, string>, bool, boost::hash<pair<size_t, string>>> checked_item1;
                                                            // [itemNo, traAtt(:区切り)] -> exists
        unordered_map<vector<string>, size_t, boost::hash<vector<string>>> itemNo4node_map;
                                                            // [node] -> coitemsをカウントする代表itemNo
        // itemNo(2)の属性(vnode2)から、requestで指定された粒度で対象itemNoを拡張する(itemInTheAtt2)
        Ewah itemInTheAtt2 = mt_occ->itemAtt->bmpList.GetVal(query.granularity.second, vnode2);
        itemInTheAtt2 = itemInTheAtt2 & query.itemFilter;
        for (auto at2 = itemInTheAtt2.begin(), eat2 = itemInTheAtt2.end(); at2 != eat2; at2++) {
            if (isTimeOut) {stat = 2; break;}
            Ewah* tra_i2_tmp;
            mt_occ->bmpList.GetVal(mt_occ->occKey, mt_occ->itemAtt->item[*at2], tra_i2_tmp);
            Ewah tra_i2 = *tra_i2_tmp & tarTraBmp;
            
            for (auto t2 = tra_i2.begin(), et2 = tra_i2.end(); t2 != et2; t2++) {
                if (isTimeOut) {stat = 2; break;}
                vector<string> vTraAtt;
                mt_occ->traAtt->traNo2traAtt(*t2, query.granularity.first, vTraAtt);
                string traAtt = Cmn::CsvStr::Join(vTraAtt, ":");
                
                Ewah item_i1;
                if (isTraGranu) {
                    // t2の属性から、requestで指定された粒度で対象トランザクションを拡張した上で、対象itemNo(1)を拡張する
                    mt_occ->expandItemByGranu(*t2, query.granularity.first, tarTraBmp,
                                              query.itemFilter, item_i1, ex_occ_cacheOnceQuery);
//                    item_i1 = item_i1 & query.itemFilter;
//                    cerr << "!!!"; Cmn::CheckEwah(item_i1);
                } else {
                    item_i1 = mt_occ->occ[*t2] & query.itemFilter;
                }
                for (auto i1 = item_i1.begin(), ei1 = item_i1.end(); i1 != ei1; i1++) {
                    if (isTimeOut) {stat = 2; break;}
                    if (*i1 >= *i2) break;
                    
                    vector<string> vnode1 = mt_occ->itemAtt->key2att(*i1, query.granularity.second);
                    string node1 = Cmn::CsvStr::Join(vnode1, ":");
                    if (checked_node1.find({node1, traAtt}) == checked_node1.end()) {
                        checked_node1[{node1, traAtt}] = true;
                    } else {
                        continue;
                    }
                    
                    // itemNo(1)が持つ属性(vnode1)から、requestで指定された粒度で対象itemNoを拡張する(itemInTheAtt1)
                    Ewah itemInTheAtt1 = mt_occ->itemAtt->bmpList.GetVal(query.granularity.second, vnode1);
                    itemInTheAtt1 = itemInTheAtt1 & query.itemFilter;
                    for (auto at1 = itemInTheAtt1.begin(), eat1 = itemInTheAtt1.end(); at1 != eat1; at1++) {
                        if (isTimeOut) {stat = 2; break;}
                        size_t itemNo4node;
                        if (isNodeGranu) {
                            // node1における代表itemNoとして拡張前のitemNo(1)を設定する
                            if (itemNo4node_map.find(vnode1) == itemNo4node_map.end()) {
                                itemNo4node_map[vnode1] = *i1;
                                itemNo4node = *i1;
                            } else {
                                itemNo4node = itemNo4node_map[vnode1];
                            }
                            //
                            if (checked_item1.find({itemNo4node, traAtt}) == checked_item1.end()) {
                                checked_item1[{itemNo4node, traAtt}] = true;
                            } else {
                                continue;
                            }
                        } else {
                            itemNo4node = *i1;
                        }
                        
                        coitems[itemNo4node]++;
                    }
                }
            }
        }
        
        const string delim = ":";
        for (auto i1 = coitems.begin(), ei1 = coitems.end(); i1 != ei1; i1++) {
            if (isTimeOut) {stat = 2; break;}
            string item1, item2;
            string itemName1, itemName2;
            for (auto& f : query.granularity.second) {
                if (f == mt_config->traFile.itemFld) {
                    item1 += mt_occ->itemAtt->item[i1->first] + delim;
                    item2 += mt_occ->itemAtt->item[*i2] + delim;
                    itemName1 += mt_occ->itemAtt->itemName[i1->first] + delim;
                    itemName2 += mt_occ->itemAtt->itemName[*i2] + delim;
                } else {
                    item1 += mt_occ->itemAtt->key2att(i1->first, f) + delim;
                    item2 += mt_occ->itemAtt->key2att(*i2, f) + delim;
                    string tmp1, tmp2;
                    mt_occ->itemAtt->code2name(f, mt_occ->itemAtt->key2att(i1->first, f), tmp1);
                    mt_occ->itemAtt->code2name(f, mt_occ->itemAtt->key2att(*i2, f), tmp2);
                    itemName1 += tmp1 + delim;
                    itemName2 += tmp2 + delim;
                }
            }
            Cmn::EraseLastChar(item1);
            Cmn::EraseLastChar(item2);
            Cmn::EraseLastChar(itemName1);
            Cmn::EraseLastChar(itemName2);
            
            size_t freq = i1->second;
            float sup = (float)freq / traNum;
            if (sup < query.selCond.minSup) continue;
            
            if (query.granularity.second.size() == 1 &&
                query.granularity.second[0] == mt_config->traFile.itemFld) {
                if (query.granularity.first.size() == 1 &&
                    query.granularity.first[0] == mt_config->traFile.traFld) {
                    if (itemFreq.find(i1->first) == itemFreq.end()) {
                        itemFreq[i1->first] = mt_occ->itemFreq(i1->first, tarTraBmp);
                    }
                    if (itemFreq.find(*i2) == itemFreq.end()) {
                        itemFreq[*i2] = mt_occ->itemFreq(*i2, tarTraBmp);
                    }
                } else {
                    if (itemFreq.find(i1->first) == itemFreq.end()) {
                        itemFreq[i1->first] = mt_occ->itemFreq(i1->first, tarTraBmp, &tra2key);
                    }
                    if (itemFreq.find(*i2) == itemFreq.end()) {
                        itemFreq[*i2] = mt_occ->itemFreq(*i2, tarTraBmp, &tra2key);
                    }
                }
            } else {
                vector<string> attVal1 = mt_occ->itemAtt->key2att(i1->first, query.granularity.second);
                vector<string> attVal2 = mt_occ->itemAtt->key2att(*i2, query.granularity.second);
                if (query.granularity.first.size() == 1 &&
                    query.granularity.first[0] == mt_config->traFile.traFld) {
                    if (itemFreq.find(i1->first) == itemFreq.end()) {
                        if (isTraGranu) {
                            itemFreq[i1->first] = mt_occ->attFreq(query.granularity.second[0],
                                                                  attVal1[0], tarTraBmp, &tra2key);
                        } else {
                            itemFreq[i1->first] = mt_occ->attFreq(query.granularity.second[0],
                                                                  attVal1[0], tarTraBmp);
                        }
                    }
                    if (itemFreq.find(*i2) == itemFreq.end()) {
                        if (isTraGranu) {
                            itemFreq[*i2] = mt_occ->attFreq(query.granularity.second[0], attVal2[0],
                                                            tarTraBmp, &tra2key);
                        } else {
                            itemFreq[*i2] = mt_occ->attFreq(query.granularity.second[0], attVal2[0], tarTraBmp);
                        }
                    }
                } else {
                    if (itemFreq.find(i1->first) == itemFreq.end()) {
                        itemFreq[i1->first] = mt_occ->attFreq(query.granularity.second, attVal1,
                                                              tarTraBmp, query.itemFilter, &tra2key);
                    }
                    if (itemFreq.find(*i2) == itemFreq.end()) {
                        itemFreq[*i2] = mt_occ->attFreq(query.granularity.second, attVal2,
                                                        tarTraBmp, query.itemFilter, &tra2key);
                    }
                }
            }
            float conf1 = (float)freq / itemFreq[i1->first];
            if (conf1 < query.selCond.minConf) continue;
//            float conf2 = (float)freq / itemFreq[*i2];
            
            float jac = (float)freq / (itemFreq[i1->first] + itemFreq[*i2] - freq);
            if (jac < query.selCond.minJac) continue;
            double lift = (double)(freq * traNum) / (itemFreq[i1->first] * itemFreq[*i2]);
            if (lift < query.selCond.minLift) continue;
            
            float pmi = Cmn::calcPmi(freq, itemFreq[i1->first], itemFreq[*i2], traNum);
            if (pmi < query.selCond.minPMI) continue;
            
            char msg[1024];
            // node1,node2,frequency,frequency1,frequency2,total,support,confidence,lift,jaccard,PMI,node1n,node2n
            const char* fmt = "%s,%s,%d,%d,%d,%d,%.5f,%.5f,%.5lf,%.5f,%.5f,%s,%s";
            sprintf(msg, fmt, item1.c_str(), item2.c_str(), freq, itemFreq[i1->first], itemFreq[*i2], traNum,
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
        res[""] = kgmod::Enum(query, mt_occ->liveTra);
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

void kgmod::exec::axisValsList(axis_t& flds, vector<vector<pivAtt_t>>& valsList) {
    valsList.reserve(flds.size());
    for (size_t i = 0; i < flds.size(); i++) {
        vector<pivAtt_t> vals;
        vector<string> tmp;
        if (flds[i].first == 'T') {
            tmp = mt_occ->bmpList.EvalKeyValue(flds[i].second);
            vals.reserve(tmp.size());
        } else if (flds[i].first == 'I') {
            tmp = mt_occ->itemAtt->bmpList.EvalKeyValue(flds[i].second);
            vals.reserve(tmp.size());
        }
        
        for (auto& t : tmp) {
            pivAtt_t att = {flds[i].first, t};
            vals.push_back(att);
        }
        valsList.push_back(vals);
    }
}

void kgmod::exec::combiAtt(vector<vector<pivAtt_t>>& valsList, vector<vector<pivAtt_t>>& hdr, vector<pivAtt_t> tmp) {
    size_t level = tmp.size();
    if (level == 0) {
        size_t hdrCnt = 1;
        for (auto& vals : valsList) hdrCnt *= vals.size();
        hdr.reserve(hdrCnt);
        tmp.reserve(valsList.size());
    } else if (level == valsList.size()) {
        hdr.push_back(tmp);
        return;
    }
    for (auto& val : valsList[level]) {
        vector<pivAtt_t> tmp2 = tmp;
        tmp2.push_back(val);
        combiAtt(valsList, hdr, tmp2);
    }
}

void kgmod::exec::nodestat(NodeStat& nodestat, map<string, Result>& res) {
    cerr << "start nodestat" << endl;
    
    signed int stat = 0;
    string header = Cmn::CsvStr::Join(nodestat.granularity.second, ":");
    for (auto& v : nodestat.vals) {
        header += ",";
        header += v.second;
    }
    
    string line;
    string traHeader = "";
    res[""].insert(make_pair(0, header));
    
    Ewah itemBmp;
    mt_occ->itemAtt->bmpList.GetVal(nodestat.granularity.second, nodestat.itemVal, itemBmp);
    itemBmp = itemBmp & nodestat.itemFilter;
    string itemVal = Cmn::CsvStr::Join(nodestat.itemVal, ":");
    size_t cnt = mt_factTable->aggregate({traHeader, nodestat.traFilter}, {itemVal, itemBmp},
                                         nodestat.vals, line);
    res[""].insert(make_pair(1, line));
    
    string buf = "status:" + to_string(stat);
    buf += ",sent:" + to_string(cnt);
    cerr << buf << endl;
    res[""].insert(make_pair(-FLT_MAX, buf));
}

void kgmod::exec::nodeimage(NodeImage& nodeimage, map<string, Result>& res) {
    cerr << "start nodeimage" << endl;
    
    vector<string> imageList;
    Ewah tmpItemBmp;
    Ewah itemBmp;
    mt_occ->itemAtt->bmpList.GetVal(nodeimage.granularity.second, nodeimage.itemVal, tmpItemBmp);
    tmpItemBmp = tmpItemBmp & nodeimage.itemFilter;
    mt_occ->filterItemBmpByTraBmp(tmpItemBmp, nodeimage.traFilter, itemBmp);
    mt_occ->itemAtt->getImageList(itemBmp, imageList);
    for (size_t i = 0; i < imageList.size(); i++) {
        res[""].insert(make_pair(i + 1, imageList[i]));
    }
    
    string buf = "status:0,sent:" + to_string(imageList.size());
    buf += ",hit:" + to_string(imageList.size());
    cerr << buf << endl;
    res[""].insert(make_pair(-FLT_MAX, buf));
    res[""].insert(make_pair(0, mt_config->itemAttFile.imageField));
}

void kgmod::exec::worksheet(WorkSheet& worksheet, map<string, Result>& res) {
    cerr << "start worksheet" << endl;
    
    signed int stat = 0;
    vector<vector<pivAtt_t>> traValsList;
    axisValsList(worksheet.traAtt, traValsList);
    vector<pivAtt_t> work0;
    vector<vector<pivAtt_t>> traHdrs;
    combiAtt(traValsList, traHdrs, work0);
    vector<Ewah> traBmp(traHdrs.size());
    for (size_t i = 0; i < traHdrs.size(); i++) {
        if (isTimeOut) {stat = 2; break;}
        traBmp[i].padWithZeroes(mt_occ->traAtt->traMax + 1);
        traBmp[i].inplace_logicalnot();
        for (size_t j = 0; j < traHdrs[i].size(); j++) {
            if (isTimeOut) {stat = 2; break;}
            Ewah* tmp;
            mt_occ->bmpList.GetVal(worksheet.traAtt[j].second, traHdrs[i][j].second, tmp);
            traBmp[i] = traBmp[i] & *tmp;
        }
    }
    
    vector<vector<pivAtt_t>> itemValsList;
    axisValsList(worksheet.itemAtt, itemValsList);
    vector<pivAtt_t> work1;
    vector<vector<pivAtt_t>> itemHdrs;
    combiAtt(itemValsList, itemHdrs, work1);
    vector<Ewah> itemBmp(itemHdrs.size());
    for (size_t i = 0; i < itemHdrs.size(); i++) {
        if (isTimeOut) {stat = 2; break;}
        itemBmp[i].padWithZeroes(mt_occ->itemAtt->itemMax + 1);
        itemBmp[i].inplace_logicalnot();
        for (size_t j = 0; j < itemHdrs[i].size(); j++) {
            if (isTimeOut) {stat = 2; break;}
            Ewah* tmp;
            mt_occ->itemAtt->bmpList.GetVal(worksheet.itemAtt[j].second, itemHdrs[i][j].second, tmp);
            itemBmp[i] = itemBmp[i] & *tmp;
        }
    }
    
    string line1;
    size_t lnum = 0;
    for (auto& fld : worksheet.traAtt) {
        line1 += fld.second + ",";
    }
    for (auto& fld : worksheet.itemAtt) {
        line1 += fld.second + ",";
    }
    if (worksheet.vals.size() == 0) {
        line1 += mt_factTable->valNames();
    } else {
        for (auto& f : worksheet.vals) {
            line1 += f.second + ",";
        }
        Cmn::EraseLastChar(line1);
    }
    res[""].insert(make_pair(lnum++, line1));
    
    cerr << "extracting" << endl;
    size_t cnt = 0;
    for (size_t t = 0; t < traBmp.size(); t++) {
        if (isTimeOut) {stat = 2; break;}
        string traHeader;
        for (auto& h : traHdrs[t]) {
            traHeader += h.second + ",";
        }
        Cmn::EraseLastChar(traHeader);
        cerr << traHeader << ":";
        for (size_t i = 0; i < itemBmp.size(); i++) {
            if (isTimeOut) {stat = 2; break;}
            string line;
            string itemHeader;
            for (auto& h : itemHdrs[i]) {
                itemHeader += h.second + ",";
            }
            Cmn::EraseLastChar(itemHeader);
            
            size_t c = mt_factTable->aggregate({traHeader, traBmp[t]}, {itemHeader, itemBmp[i]}, worksheet.vals, line);
            if (c != 0) {
                cnt += c;
//                cerr << itemHeader << " ";
                res[""].insert(make_pair(lnum++, line));
            }
        }
        cerr << endl;
    }
    
    string buf = "status:" + to_string(stat);
    buf += ",sent:" + to_string(cnt);
//    buf += ",hit:" + to_string(hit);
    cerr << buf << endl;
    res[""].insert(make_pair(-FLT_MAX, buf));
}

void kgmod::exec::pivot(Pivot& pivot, map<string, Result>& res) {
    cerr << "start pivot" << endl;
    vector<vector<vector<pivAtt_t>>> valsList(2);    // [0] -> X axis, [1] -> Y axis
    vector<vector<vector<pivAtt_t>>> hdrs(2);        // [0] -> X axis, [1] -> Y axis
    vector<map<vector<pivAtt_t>, Ewah>> bmps(2);      // [0] -> X axis, [1] -> Y axis
    
    // xy: [0] -> X axis, [1] -> Y axis
    for (size_t xy = 0; xy < 2; xy++) {
        axisValsList(pivot.axes[xy], valsList[xy]);
        vector<pivAtt_t> work;
        combiAtt(valsList[xy], hdrs[xy], work);
        
        for (auto vals = hdrs[xy].begin(), evals = hdrs[xy].end(); vals != evals; vals++) {
            bmps[xy][*vals].padWithZeroes(mt_occ->traAtt->traMax + 1);
            bmps[xy][*vals].inplace_logicalnot();
            for (size_t i = 0, ei = vals->size(); i < ei; i++) {
                if (isTimeOut) return;
                Ewah bmp;
                if ((*vals)[i].first == 'T') {
                    mt_occ->bmpList.GetVal(pivot.axes[xy][i].second, (*vals)[i].second, bmp);
                } else if ((*vals)[i].first == 'I') {
                    mt_occ->item2traBmp(pivot.axes[xy][i].second, (*vals)[i].second, bmp);
                }
                bmps[xy][*vals] = bmps[xy][*vals] & bmp;
            }
        }
    }
    
    cerr << "calculating matrix" << endl;
    map<pair<vector<pivAtt_t>, vector<pivAtt_t>>, float> mat;
    vector<map<vector<pivAtt_t>, bool>> axesHeader(2);
    for (auto& x : bmps[0]) {
        for (auto& y : bmps[1]) {
            if (isTimeOut) return;
            Ewah cross = x.second & y.second;
            cross = cross & pivot.traFilter;
            size_t num = cross.numberOfOnes();
            if ((float)num > pivot.cutoff) {
                mat[{x.first, y.first}] = (float)num;
                axesHeader[0][x.first] = true;
                axesHeader[1][y.first] = true;
            }
        }
    }
    
    // enumerating header
    float lnum = 0;
    string lineHead;
    for (size_t i = 0; i < pivot.axes[1].size(); i++) lineHead += ",";
    bool isFirstOne = true;
    vector<string> lines(pivot.axes[0].size());
    for (size_t i = 0; i < lines.size(); i++) lines[i] = lineHead;
    for (auto x_hfld = axesHeader[0].begin(), ex_hfld = axesHeader[0].end(); x_hfld != ex_hfld; x_hfld++) {
        size_t c = 0;
        for (auto x_hdr = x_hfld->first.begin(), ex_hdr = x_hfld->first.end(); x_hdr != ex_hdr; x_hdr++) {
            if (isTimeOut) return;
            if (! isFirstOne) lines[c] += ",";
            lines[c] += x_hdr->second;
            c++;
        }
        isFirstOne = false;
    }
    
    for (size_t i = 0; i < lines.size(); i++) {
        res[""].insert(make_pair(lnum, lines[i]));
        lnum++;
    }
    
    // enumerating result
    cerr << "formatting matrix" << endl;
    for (auto y_hdr = axesHeader[1].begin(), ey_hdr = axesHeader[1].end(); y_hdr != ey_hdr; y_hdr++) {
        string line;
        for (auto y_hfld = y_hdr->first.begin(), ey_hfld = y_hdr->first.end(); y_hfld != ey_hfld; y_hfld++) {
            line += y_hfld->second; line += ",";
        }
        bool isFirst = true;
        for (auto x_hdr = axesHeader[0].begin(), ex_hdr = axesHeader[0].end(); x_hdr != ex_hdr; x_hdr++) {
            if (isTimeOut) return;
            if (isFirst) isFirst = false;
            else line += ",";
            size_t cnt;
            if (mat.find({x_hdr->first, y_hdr->first}) == mat.end()) {
                cnt = 0;
            } else {
                cnt = mat[{x_hdr->first, y_hdr->first}];
            }
            if (cnt == 0) line += "";
            else line += to_string(cnt);
        }
        res[""].insert(make_pair(lnum, line));
    }
}

void kgmod::exec::saveFilters(Query& query) {
    ofstream traFile(mt_config->outDir + "/trafilter.csv", ios::out);
    traFile << mt_config->traFile.traFld << "\n";
    for (auto i = query.traFilter.begin(), ei = query.traFilter.end(); i != ei; i++) {
        traFile << mt_occ->traAtt->tra[*i] << "\n";
    }
    traFile.close();
    
    ofstream itemFile(mt_config->outDir + "/itemfilter.csv", ios::out);
    itemFile << mt_config->traFile.itemFld << "\n";
    for (auto i = query.itemFilter.begin(), ei = query.itemFilter.end(); i != ei; i++) {
        itemFile << mt_occ->itemAtt->item[*i] << "\n";
    }
    itemFile.close();
}

void kgmod::exec::co_occrence_mcmd(Query& query) {
    int stat;
    // mcommon i=../bra3/receiptJAN.csv m=traFilter.csv k=receitID o=xxbase0
    stringstream cmd0;
    cmd0 << "/usr/local/bin/mjoin m=" << mt_config->traAttFile.name << " k=" << mt_config->traFile.traFld;
    cmd0 << " i=" << mt_config->traFile.name << " | ";
    cmd0 << "/usr/local/bin/mcommon m=" << mt_config->outDir << "/traFilter.csv k=" << mt_config->traFile.traFld;
    cmd0 << " o=" << mt_config->outDir << "/xxbase0";
    cerr << "exec: " << cmd0.str() << endl;
    stat = system(cmd0.str().c_str());
    if (stat != 0) {cerr << "#ERROR# failed in " << cmd0.str() << endl; return;}
    
    // mjoin i=xxbase0 m=../bra3/receipt.csv k=receiptID |\
    // mjoin m=../bra3/jan.csv k=JAN |\
    // mcommon m=itemFilter.csv k=JAN |\
    // mcut f="MemberID,ProductNo" |\
    // muniq k="MemberID,ProductNo" o=xxbase
    stringstream cmd1;
    cmd1 << "/usr/local/bin/mcommon i=" << mt_config->outDir << "/xxbase0 m=";
    cmd1 << mt_config->outDir << "/itemFilter.csv k=" << mt_config->traFile.itemFld << " | ";
    cmd1 << "/usr/local/bin/mjoin m=" << mt_config->itemAttFile.name;
    cmd1 << " k=" << mt_config->traFile.itemFld << " | ";
    cmd1 << "/usr/local/bin/mcut f='" << Cmn::CsvStr::Join(query.granularity.first) << ",";
    cmd1 << Cmn::CsvStr::Join(query.granularity.second) << "' | ";
    cmd1 << "/usr/local/bin/muniq k='" << Cmn::CsvStr::Join(query.granularity.first) << ",";
    cmd1 << Cmn::CsvStr::Join(query.granularity.second);
    cmd1 << "' o=" << mt_config->outDir << "/xxbase";
    cerr << "exec: " << cmd1.str() << endl;
    stat = system(cmd1.str().c_str());
    if (stat != 0) {cerr << "#ERROR# failed in " << cmd1.str() << endl; return;}
    
    // muniq f=MemberID i=xxbase0 | mcount a=total o=xxtotal
    stringstream cmd2;
    cmd2 << "/usr/local/bin/muniq k=" << Cmn::CsvStr::Join(query.granularity.first);
    cmd2 << " i=" << mt_config->outDir << "/xxbase0 | ";
    cmd2 << "/usr/local/bin/mcount a=total o=" << mt_config->outDir << "/xxtotal";
    cerr << "exec: " << cmd2.str() << endl;
    stat = system(cmd2.str().c_str());
    if (stat != 0) {cerr << "#ERROR# failed in " << cmd2.str() << endl; return;}

    stringstream cmd3;
    cmd3 << "/usr/local/bin/mcombi k=" << Cmn::CsvStr::Join(query.granularity.first);
    cmd3 << " f=" << Cmn::CsvStr::Join(query.granularity.second);
    cmd3 << " a=node1,node2 n=2 i=" << mt_config->outDir << "/xxbase | ";
    cmd3 << "/usr/local/bin/mcount k=node1,node2 a=frequency o=" << mt_config->outDir << "/xxcombi2";
    cerr << "exec: " << cmd3.str() << endl;
    stat = system(cmd3.str().c_str());
    if (stat != 0) {cerr << "#ERROR# failed in " << cmd3.str() << endl; return;}
    
    // mcount k=MemberID a=nfreq i=xxbase o=xxcnt
    stringstream cmd4;
    cmd4 << "/usr/local/bin/mcount k=" << Cmn::CsvStr::Join(query.granularity.second);
    cmd4 << " a=nfreq i=";
    cmd4 << mt_config->outDir <<"/xxbase o=" << mt_config->outDir << "/xxcnt";
    cerr << "exec: " << cmd4.str() << endl;
    stat = system(cmd4.str().c_str());
    if (stat != 0) {cerr << "#ERROR# failed in " << cmd4.str() << endl; return;}
    
    // mjoin k=node1 K=ProductNo f=nfreq:frequency1 m=xxcnt i=xxcombi2 |\
    // mjoin k=node2 K=ProductNo f=nfreq:frequency2 m=xxcnt |\
    // mproduct f=total m=xxtotal |\
    // mcal c='format(${frequency}/${total},"%.5f")' a=support |\
    // mcal c='format(${frequency} / (${frequency1} + ${frequency2}} - ${frequency} ),"%.5f")' a=jac
    // mcal c='format((${frequency} * ${total}) / (${frequency1} * ${frequency2}),"%.5f")' a=lift
    // mcal c='format(ln((${frequency} / ${total}) / ((${frequency1} / ${total}) * ${frequency2} / ${total})) / (-1) * ln((${frequency} / ${total})),"%.5f")' a=PMI |\
    // mcut f=item1,item2,frequency,frequency1,frequency2,total |\
    // msortf f=frequency |\
    // mfldname -q o=test.csv
    stringstream cmd5;
    cmd5 << "/usr/local/bin/mjoin k=node1 K=" << Cmn::CsvStr::Join(query.granularity.second);
    cmd5 << " f=nfreq:frequency1 m=" << mt_config->outDir << "/xxcnt i=";
    cmd5 << mt_config->outDir << "/xxcombi2 | ";
    cmd5 << "/usr/local/bin/mjoin k=node2 K=" << Cmn::CsvStr::Join(query.granularity.second);
    cmd5 << " f=nfreq:frequency2 m=" << mt_config->outDir << "/xxcnt | ";
    cmd5 << "/usr/local/bin/mproduct f=total m=";
    cmd5 << mt_config->outDir << "/xxtotal | ";
    cmd5 << "/usr/local/bin/mcal c='format(${frequency}/${total},\"%.5f\")' a=support | ";
    cmd5 << "/usr/local/bin/mcal c='format(${frequency}/${frequency1},\"%.5f\")' a=confidence | ";
    cmd5 << "/usr/local/bin/mcal c='format(${frequency} / (${frequency1} + ${frequency2} - ${frequency}),\"%.5f\")' a=jaccard | ";
    cmd5 << "/usr/local/bin/mcal c='format((${frequency} * ${total}) / (${frequency1} * ${frequency2}),\"%.5f\")' a=lift | ";
    cmd5 << "/usr/local/bin/mcal c='format((ln((${frequency} / ${total}) / ((${frequency1} / ${total}) * (${frequency2} / ${total})))) / ((-1) * ln(${frequency} / ${total})),\"%.5f\")' a=PMI | ";
    cmd5 << "/usr/local/bin/msel c='and(${support}>=" << query.selCond.minSup;
    cmd5 << ",${confidence}>=" << query.selCond.minConf << ",${jaccard}>=" << query.selCond.minJac;
    cmd5 << ",${lift}>=" << query.selCond.minLift << ",${PMI}>=" << query.selCond.minPMI << ")' | ";
    cmd5 << "/usr/local/bin/mcut f=node1,node2,frequency,frequency1,frequency2,total,support,confidence,jaccard,lift,PMI | ";
    cmd5 << "/usr/local/bin/msortf f=frequency%nr | /usr/local/bin/mfldname -q o=" << mt_config->outDir << "/result.csv";
    cerr << "exec: " << cmd5.str() << endl;
    stat = system(cmd5.str().c_str());
    if (stat != 0) {cerr << "#ERROR# failed in " << cmd5.str() << endl; return;}
}

void kgmod::exec::diff_res_vs_mcmd(void) {
    
}

void kgmod::exec::doControl(EtcReq& etcReq) {
    string res_body;
    if (boost::iequals(etcReq.func, "bye")) {
        cerr << "bye message recieved" << endl;
        closing_ = true;
        res_body = "terminated\n";
    } else if (boost::iequals(etcReq.func, "config")) {
        cerr << "bye message recieved" << endl;
        string configJson;
        if (mt_config->getJson(configJson)) {
            res_body = "status:0\n";
            res_body += configJson;
        } else {
            res_body = "status:-1\nFailed to convert json";
        }
    } else {
        cerr << "unknown control request" << endl;
        res_body = "unknown control request\n";
    }
    put_send_data(res_body);
    Http::proc();
}

void kgmod::exec::doRetrieve(EtcReq& etcReq) {
    string res_body;
    cerr << etcReq.func << endl;
    if (boost::iequals(etcReq.func, "ListTraAtt")) {
        res_body ="TraAtt\n";
        vector<string> traAtts = mt_occ->traAtt->listAtt();
        for (auto att = traAtts.begin(); att != traAtts.end(); att++) {
            res_body += *att; res_body += "\n";
        }
//    } else if (boost::iequals(etcReq.func, "ListItemAtt")) {
    } else if (boost::iequals(etcReq.func, "GetTraAtt")) {
        res_body = etcReq.func;  res_body += "\n";
        vector<string> attVal = mt_occ->evalKeyValue(etcReq.args[0]);
        for (auto att = attVal.begin(); att != attVal.end(); att++) {
            res_body += *att; res_body += "\n";
        }
//    } else if (boost::iequals(etcReq.func, "GetItemAtt")) {
    } else {
        cerr << "unknown control request" << endl;
        res_body = "unknown control request\n";
    }
    put_send_data(res_body);
    Http::proc();
}

void kgmod::exec::proc(void) {
    try {
        chrono::system_clock::time_point timeStart;
        chrono::system_clock::time_point timeEnd;
        double elapsedTime;
        timeStart = chrono::system_clock::now();
        Request request(mt_config, mt_occ, mt_factTable, golap_->fil);
        request.evalRequest(req_body());
        timeEnd = chrono::system_clock::now();
        elapsedTime = chrono::duration_cast<chrono::milliseconds>(timeEnd - timeStart).count();
        cerr << "filter eval time: " << elapsedTime / 1000 << " sec" << endl;
        
        // Excecuting Enum on each dimention
        map<string, Result> res;
        timeStart = chrono::system_clock::now();
        
        // 重たい処理の場合、timerによってisTimeOutがfalseからtrueに変えられる
        // 各ファンクション内のループの先頭でisTimeOutをチェックしtreeの場合ループを強制的に抜ける
        setTimer(request.deadlineTimer);
        if (request.mode == "control") {
            doControl(request.etcRec);
        } else if (request.mode == "retrieve") {
            doRetrieve(request.etcRec);
        } else if (request.mode == "query") {
            co_occurrence(request.query, res);
        } else if (request.mode == "nodestat") {
            nodestat(request.nodestat, res);
        } else if (request.mode == "nodeimage") {
            nodeimage(request.nodeimage, res);
        } else if (request.mode == "worksheet") {
            worksheet(request.worksheet, res);
        } else if (request.mode == "pivot") {
            pivot(request.pivot, res);
        }
        cancelTimer();
        
        timeEnd = chrono::system_clock::now();
        elapsedTime = chrono::duration_cast<chrono::milliseconds>(timeEnd - timeStart).count();
        cerr << "process time: " << elapsedTime / 1000 << " sec" << endl;
        
        if (request.query.debug_mode == 2) {
            if (request.query.dimension.key != "") {
                cerr << "#WARNING# dataCheck mode is not executed, if 'dimension' element is set in query" << endl;
            } else {
                saveFilters(request.query);
                co_occrence_mcmd(request.query);
            }
        }
        
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
    catch(string& msg) {
        cerr << msg << endl;
        put_send_data(msg);
        Http::proc();
        return;
    }
    catch(kgError& err){
        auto msg = err.message();
        string body = "status:-1\n";
        for (auto i = msg.begin(); i != msg.end(); i++) {
            body += *i + "\n";
        }
        put_send_data(body);
        Http::proc();
        return;
    }
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
        
        factTable = new FactTable(config, _env, occ);
        factTable->load();
        mt_factTable = factTable;   // マルチスレッド用反則技
        
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
        occ->exBmpList.save(true);
        occ->ex_occ.save(true);
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
