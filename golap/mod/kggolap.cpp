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
#include <boost/algorithm/string.hpp>
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

kgmod::kgGolap::Slice kgmod::kgGolap::makeSliceBitmap(string& cmdline) {
    boost::trim(cmdline);
    Slice out;
    if (cmdline == "") {
        out.key = "";
        return out;
    }
    vector<string> param = Cmn::CsvStr::Parse(cmdline);
    out.key = param[0];
    for (size_t i = 1; i < param.size(); i++) {
        out.SliceBmpList[param[i]] =  occ->bmpList.GetVal(out.key, param[i]);
    }
    return out;
}

kgmod::kgGolap::Result kgmod::Enum(struct selCond& selCond, sort_key sortKey,
                                   Ewah& TraBmp, Ewah& ItemBmp, string& traUniqAttKey, Ewah& sliceBmp) {
    cerr << "filtering" << endl;
    size_t hit = 0;
    bool notSort = false;
    signed int stat = 0;
    kgGolap::Result res;
    unordered_map<size_t, size_t> itemFreq;
    
    Ewah tarTraBmp = TraBmp & sliceBmp;
    size_t traNum = tarTraBmp.numberOfOnes();
    if (traNum == 0) {
        cerr << "trabitmap is empty" << endl;
        res.insert(make_pair(0, "status:0,sent:0,hit:0"));
        res.insert(make_pair(1, "trabitmap is empty"));
        return res;
    }
    
    // traUniqAttKeyを指定していた場合は、traNumをtraUniqAttKeyのデータ数で上書き
    if (traUniqAttKey != "") {
        traNum = mt_occ->countKeyValue(traUniqAttKey, &TraBmp);
    }
    
    // 重たい処理の場合、timerによってisTimeOutがfalseからtrueに変えられる
    // 下のループ処理の先頭でisTimeOutをチェックしtreeの場合ループを強制的に抜ける
    setTimer(mt_config->deadlineTimer);
    
//    vector<size_t> vec;
//    vec.reserve(ItemBmp.numberOfOnes());
    vector<string> tra2key;
    if (traUniqAttKey != "") mt_occ->getTra2KeyValue(traUniqAttKey, &tra2key);
    map<size_t, size_t> coitems;
    for (auto i = ItemBmp.begin(), ei = ItemBmp.end(); i != ei; i++) {
        if (isTimeOut) {stat = 2; break;}
        coitems.clear();
        Ewah* tra_i_tmp;
        mt_occ->bmpList.GetVal(mt_occ->occKey, mt_occ->itemAtt->item[*i], tra_i_tmp);
        Ewah tra_i;
        tra_i = *tra_i_tmp & tarTraBmp;
        
        unordered_map<pair<string, size_t>, int, boost::hash<pair<string, size_t>>> checkedAttVal;
        for (auto t = tra_i.begin(), et = tra_i.end(); t != et; t++) {
            if (isTimeOut) {stat = 2; break;}
            string& attVal = tra2key[*t];
            Ewah item_ii;
            item_ii = mt_occ->occ[*t] & ItemBmp;
            for (auto ii = item_ii.begin(), eii = item_ii.end(); ii != eii; ii++) {
                if (*ii >= *i) break;
                if (traUniqAttKey == "") {
                    coitems[*ii]++;
                } else {
                    if (checkedAttVal.find({attVal, *ii}) == checkedAttVal.end()) {
                        coitems[*ii]++;
                        checkedAttVal[{attVal, *ii}] = 1;
                    }
                }
            }
        }
        
        for (auto c = coitems.begin(), ec = coitems.end(); c != ec; c++) {
            if (isTimeOut) {stat = 2; break;}
            string item1 = mt_occ->itemAtt->item[c->first];
            string item2 = mt_occ->itemAtt->item[*i];
            string itemName1 = mt_occ->itemAtt->itemName[c->first];
            string itemName2 = mt_occ->itemAtt->itemName[*i];
            size_t freq = c->second;
            float sup = (float)freq / traNum;
            if (sup < selCond.minSup) continue;
            
            if (traUniqAttKey == "") {
                if (itemFreq.find(c->first) == itemFreq.end())
                    itemFreq[c->first] = mt_occ->itemFreq(c->first, tarTraBmp);
                if (itemFreq.find(*i) == itemFreq.end())
                    itemFreq[*i] = mt_occ->itemFreq(*i, tarTraBmp);
            } else {
                if (itemFreq.find(c->first) == itemFreq.end())
                    itemFreq[c->first] = mt_occ->itemFreq(c->first, tarTraBmp, &tra2key);
                if (itemFreq.find(*i) == itemFreq.end())
                    itemFreq[*i] = mt_occ->itemFreq(*i, tarTraBmp, &tra2key);
            }
            float conf1 = (float)freq / itemFreq[*i];
            if (conf1 < selCond.minConf) continue;
//            float conf2 = (float)freq / itemFreq[c->first];
            
            float jac = (float)freq / (itemFreq[*i] + itemFreq[c->first] - freq);
            if (jac < selCond.minJac) continue;
            float lift = (float)(freq * traNum) / (itemFreq[*i] * itemFreq[c->first]);
            if (lift < selCond.minLift) continue;
            
            float pmi = Cmn::calcPmi(freq, itemFreq[*i], itemFreq[c->first], traNum);
            if (pmi < selCond.minPMI) continue;
            
            char msg[1024];
            // node1,node2,frequency,frequency1,frequency2,total,support,confidence,lift,jaccard,PMI,node1n,node2n
            const char* fmt = "%s,%s,%d,%d,%d,%d,%f,%f,%f,%f,%f,%s,%s";
            sprintf(msg, fmt, item1.c_str(), item2.c_str(), freq, itemFreq[*i], itemFreq[c->first], traNum,
                    sup, conf1, lift, jac, pmi, itemName1.c_str(), itemName2.c_str());
            float skey;
            if (sortKey == SORT_SUP) {
                skey = -sup;
            } else if (sortKey == SORT_CONF) {
//                skey = -min(conf1,conf2);
                skey = -conf1;
            } else if (sortKey == SORT_LIFT) {
                skey = -lift;
            } else if (sortKey == SORT_JAC) {
                skey = -jac;
            } else if (sortKey == SORT_PMI) {
                skey = -pmi;
            } else {
                notSort = true;
                skey = -10;
            }
            res.insert(make_pair(skey, string(msg)));

            if (res.size() > mt_config->sendMax) {
                auto pos = res.end();
                pos--;
                res.erase(pos);
                stat = 1;
                if (notSort) break;
            }
            hit++;
        }
    }
    // タイマをキャンセル
    cancelTimer();

    const string csvHeader = "node1,node2,frequency,frequency1,frequency2,total,support,confidence,lift,jaccard,PMI,node1n,node2n";
    string buf = "status:" + to_string(stat);
    buf += ",sent:" + to_string(res.size());
    buf += ",hit:" + to_string(hit);
    cerr << buf << endl;
    buf += "\n";
    buf += csvHeader;
    res.insert(make_pair(-FLT_MAX, buf));
    return res;
}

//***
void kgmod::MT_Enum(mq_t* mq, struct selCond selCond, sort_key sortKey, Ewah TraBmp, Ewah ItemBmp,
                    string traUniqAttKey, map<string, kgGolap::Result>* res) {
    mq_t::th_t* T = mq->pop();
    while (T != NULL) {
        cerr << "#" << T->first << ") tarTra:"; Cmn::CheckEwah(T->second.second);
        kgGolap::Result rr = Enum(selCond, sortKey, TraBmp, ItemBmp, traUniqAttKey, *(T->second.second));
        (*res)[T->second.first] = rr;
        delete T->second.second;
        delete T;
        T = mq->pop();
    }
}
//**/

void kgmod::exec::proc(void) {
    string line;
    stringstream ss;
    get_receive_buff(ss);
    while (getline(ss, line)) if (line.size() == 1) break;
    
    int c = 0;
    Ewah TraBmp;
    Ewah ItemBmp;
    selCond selCond;
    string sortCond;
    enum sort_key sortKey = SORT_NONE;
    string traUniqAttKey;
    kgGolap::Slice slice;
    
    while (getline(ss, line)) {
        Cmn::chomp(line);
        vector<string> vec = Cmn::CsvStr::Parse(line);
        if (boost::iequals(line, "bye")) {
            cerr << "bye message recieved" << endl;
            closing_ = true;
            string body = "terminated\n";
            put_send_data(body);
            Http::proc();
            return;
        } else if (boost::iequals(line, "ListTraAtt")) {
            cerr << vec[0] << endl;
            string body ="TraAtt\n";
            vector<string> traAtts = mt_occ->traAtt->listAtt();
            for (auto att = traAtts.begin(); att != traAtts.end(); att++) {
                body += *att; body += "\n";
            }
            put_send_data(body);
            Http::proc();
            return;
        } else if (vec.size() != 0 && boost::iequals(vec[0], "GetTraAtt")) {
            cerr << vec[0] << endl;
            string body = vec[1];  body += "\n";
            vector<string> attVal = mt_occ->evalKeyValue(vec[1]);
            for (auto att = attVal.begin(); att != attVal.end(); att++) {
                body += *att; body += "\n";
            }
            put_send_data(body);
            Http::proc();
            return;
        }
        
        try {
            if (c == 0) {
                TraBmp = golap_->fil->makeTraBitmap(line);
            } else if (c == 1) {
                ItemBmp = golap_->fil->makeItemBitmap(line);
            } else if (c == 2) {
                for (size_t i = vec.size(); i < 5; i++) {
                    vec.resize(i + 1);
                    if (i == 4) vec[i] = "-1";
                    else vec[i] = "0";
                }
                selCond = {stod(vec[0]), stod(vec[1]), stod(vec[2]), stod(vec[3]), stod(vec[4])};
                selCond.dump();
            } else if (c == 3) {
                // sortCond
                if (boost::iequals(line, "SUP")) {
                    sortKey = SORT_SUP;
                } else if (boost::iequals(line, "CONF")) {
                    sortKey = SORT_CONF;
                } else if (boost::iequals(line, "LIFT")) {
                    sortKey = SORT_LIFT;
                } else if (boost::iequals(line, "JAC")) {
                    sortKey = SORT_JAC;
                } else if (boost::iequals(line, "PMI")) {
                    sortKey = SORT_PMI;
                } else {
                    sortKey = SORT_NONE;
                }
                cerr << "sortCond: " << sortCond << endl;
            } else if (c == 4) {
                traUniqAttKey = line;
                cerr << "traUniqAttKey: " << traUniqAttKey << endl;
            } else if (c == 5) {
                slice = golap_->makeSliceBitmap(line);
                cerr << "slice: " << line << endl;
            } else if (c == 6) {
                // デバッグ用
                if (boost::iequals(line, "notExec")) {
                    string body = line;
                    put_send_data(body);
                    Http::proc();
                    return;
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
            return;
        }
    }
    
    // Excecuting Enum on each slice
    chrono::system_clock::time_point Start = chrono::system_clock::now();
    map<string, kgGolap::Result> res;
    Ewah allTraBmp;
    allTraBmp.padWithZeroes(golap_->occ->traAtt->traMax);
    allTraBmp = allTraBmp.logicalnot();
    
    if (slice.SliceBmpList.size() == 0) {
        res[""] = kgmod::Enum(selCond, sortKey, TraBmp, ItemBmp, traUniqAttKey, allTraBmp);
    } else {
        mq_t::th_t *th;
        if (mt_config->mt_enable) {
            mq_t mq;
            size_t threadNo = 0;
            for (auto i = slice.SliceBmpList.begin(); i != slice.SliceBmpList.end(); i++) {
                th = new mq_t::th_t;
                th->first = threadNo;
                th->second.first = i->first;
                th->second.second = new Ewah;
                th->second.second->expensive_copy(i->second);
                mq.push(th);
            }
            
            vector<boost::thread> thg;
            for (int i = 0; i < mt_config->mt_degree; i++) {
                thg.push_back(boost::thread([&mq, selCond, sortKey, TraBmp, ItemBmp, traUniqAttKey, &res] {
                    MT_Enum(&mq, selCond, sortKey, TraBmp, ItemBmp, traUniqAttKey, &res);
                }));
            }
            
            for (boost::thread& th : thg) {
                th.join();
            }
        } else {
            for (auto i = slice.SliceBmpList.begin(); i != slice.SliceBmpList.end(); i++) {
                res[i->first] = kgmod::Enum(selCond, sortKey, TraBmp, ItemBmp, traUniqAttKey, i->second);
            }
        }
    }
    chrono::system_clock::time_point End = chrono::system_clock::now();
    double elapsed = chrono::duration_cast<chrono::milliseconds>(End-Start).count();
    cerr << "process time: " << elapsed / 1000 << " sec" <<endl;
    
    cerr << "sending" << endl;
    string body;
    for (auto i = res.begin(); i != res.end(); i++) {
        if (i->first != "") {
            body += slice.key; body += ":"; body += i->first; body += "\n";
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
