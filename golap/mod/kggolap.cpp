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
    _name    = "kgbindex";
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

kgmod::kgGolap::Result kgmod::kgGolap::Enum(struct selCond& selCond, sort_key sortKey, Ewah& TraBmp, Ewah& ItemBmp) {
    cerr << "filtering" << endl;
    size_t hit = 0;
    bool notSort = false;
    signed int stat = 0;
    Result res;
    unordered_map<size_t, size_t> itemFreq;
    
    size_t traNum = TraBmp.numberOfOnes();
    if (traNum == 0) {
        cerr << "trabitmap is empty" << endl;
        res.insert(make_pair(0,     "status: 0, sent: 0, hit: 0"));
        res.insert(make_pair(1, "trabitmap is empty"));
        return res;
    }
    
    cerr << "Target item: ";
    Cmn::CheckEwah(ItemBmp);
    
    // 重たい処理の場合、timerによってisTimeOutがfalseからtrueに変えられる
    // 下のループ処理の先頭でisTimeOutをチェックしtreeの場合ループを強制的に抜ける
    setTimer(config->deadlineTimer);
    
//    vector<size_t> vec;
//    vec.reserve(ItemBmp.numberOfOnes());
    map<size_t, size_t> coitems;
    for (auto i = ItemBmp.begin(); i != ItemBmp.end(); i++) {
        coitems.clear();
        Ewah* tra_i_tmp;
        occ->bmpList.GetVal(occ->occKey, occ->itemAtt->item[*i], tra_i_tmp);
        Ewah tra_i;
        tra_i = *tra_i_tmp & TraBmp;
        
        /************************************
        for (auto ii : vec) {
            Ewah tra_ii;
            occ->bmpList.GetVal(occ->occKey, occ->itemAtt->item[ii], tra_ii);
            Ewah cooccur;
            cooccur = TraBmp & tra_i & tra_ii;
            size_t cnt = cooccur.numberOfOnes();
            if (cnt != 0) coitems[ii] = cnt;
        }
        vec.push_back(*i);
         ************************************/
        
        for (auto t = tra_i.begin(); t != tra_i.end(); t++) {
            Ewah item_ii;
            item_ii = occ->occ[*t] & ItemBmp;
            for (auto ii = item_ii.begin(); ii != item_ii.end(); ii++) {
                if (*ii >= *i) break;
                coitems[*ii]++;
            }
        }

        for (auto c = coitems.begin(); c != coitems.end(); c++) {
            string item1 = occ->itemAtt->item[c->first];
            string item2 = occ->itemAtt->item[*i];
            string itemName1 = occ->itemAtt->itemName[c->first];
            string itemName2 = occ->itemAtt->itemName[*i];
            size_t freq = c->second;
            float sup = (float)freq / traNum;
            if (sup < selCond.minSup) break;
            
            if (itemFreq.find(*i) == itemFreq.end())
                itemFreq[*i] = occ->itemFreq(*i, TraBmp);
            if (itemFreq.find(c->first) == itemFreq.end())
                itemFreq[c->first] = occ->itemFreq(c->first, TraBmp);
            float conf1 = (float)freq / itemFreq[*i];
            if (conf1 < selCond.minConf) break;
//            float conf2 = (float)freq / itemFreq[c->first];
            
            float jac = (float)freq / (itemFreq[*i] + itemFreq[c->first] - freq);
            if (jac < selCond.minJac) break;
            float lift = (float)(freq * traNum) / (itemFreq[*i] * itemFreq[c->first]);
            if (lift < selCond.minLift) break;
            
            float pmi = Cmn::calcPmi(freq, itemFreq[*i], itemFreq[c->first], traNum);
            
            char msg[512];
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
            } else {
                notSort = true;
                skey = -10;
            }
            res.insert(make_pair(skey, string(msg)));

            if (res.size() > config->sendMax) {
                auto pos = res.end();
                pos--;
                res.erase(pos);
                stat = 1;
                if (notSort) break;
            }
            hit++;
            
            if (isTimeOut) {stat = 2; break;}
        }
        if (isTimeOut) {stat = 2; break;}
    }
    // タイマをキャンセル
    cancelTimer();

    const string csvHeader = "node1,node2,frequency,frequency1,frequency2,total,support,confidence,lift,jaccard,PMI,node1n,node2n";
    string buf = "status: " + to_string(stat);
    buf += ", sent: " + to_string(res.size());
    buf += ", hit: " + to_string(hit);
    cerr << buf << endl;
    buf += "\n";
    buf += csvHeader;
    res.insert(make_pair(-FLT_MAX, buf));
    return res;
}

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
    while (getline(ss, line)) {
        Cmn::chomp(line);
        if (line == "bye") {
            cerr << "bye message recieved" << endl;
            closing_ = true;
            put_send_data("terminated\n");
            Http::proc();
            return;
        }
        
        try {
            if (c == 0) {
                TraBmp = golap_->fil->makeTraBitmap(line);
            } else if (c == 1) {
                ItemBmp = golap_->fil->makeItemBitmap(line);
            } else if (c == 2) {
                vector<string> sel = Cmn::CsvStr::Parse(line);
                for (size_t i = sel.size(); i < 4; i++) sel[i] = "0";
                selCond = {stod(sel[0]), stod(sel[1]), stod(sel[2]), stod(sel[3])};
                selCond.dump();
            } else if (c == 3) {
                sortCond = line;
                transform(sortCond.cbegin(), sortCond.cend(), sortCond.begin(), ::toupper);
                if (sortCond == "SUP") {
                    sortKey = SORT_SUP;
                    cerr << "sortCond: " << "SUP" << endl;
                } else if (sortCond == "CONF") {
                    sortKey = SORT_CONF;
                    cerr << "sortCond: " << "CONF" << endl;
                } else if (sortCond == "LIFT") {
                    sortKey = SORT_LIFT;
                    cerr << "sortCond: " << "LIFT" << endl;
                } else if (sortCond == "JAC") {
                    sortKey = SORT_JAC;
                    cerr << "sortCond: " << "JAC" << endl;
                } else {
                    sortKey = SORT_NONE;
                    cerr << "sortCond: " << "NONE" << endl;
                }
            } else {
                break;
            }
            c++;
        } catch(kgError& err){
            auto msg = err.message();
            for (auto i = msg.begin(); i != msg.end(); i++) {
                put_send_data(*i + "\n");
                Http::proc();
            }
            return;
        }
    }
    
    chrono::system_clock::time_point Start = chrono::system_clock::now();
    kgGolap::Result res = golap_->Enum(selCond, sortKey, TraBmp, ItemBmp);
    chrono::system_clock::time_point End = chrono::system_clock::now();
    double elapsed = chrono::duration_cast<chrono::milliseconds>(End-Start).count();
    cerr << "process time: " << elapsed / 1000 << " sec" <<endl;
    
    cerr << "sending" << endl;
    put_send_data("HTTP/1.1 200 Ok\r\nContent-type: text/plain; charset=UTF-8\r\nConnection: keep-alive\r\n\r\n\n");
    Http::proc();
    for (auto i = res.begin(); i != res.end(); i++) {
        put_send_data((i->second) + "\n");
        Http::proc();
    }
}

//
int kgmod::kgGolap::run() {
    try {
        setArgs();
        config = new Config(opt_inf);
        config->dump(opt_debug);
        
        occ = new Occ(config, _env);
        occ->load();
        occ->dump(opt_debug);
        
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
