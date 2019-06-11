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

#include <kgError.h>
#include <kgMethod.h>
#include <kgConfig.h>
#include <kgMessage.h>
#include "kgCsv.h"
#include "bidx-ewah.hpp"
#include "config.hpp"
#include "param.hpp"
#include "filter.hpp"
#include "kgcregdb.hpp"

using namespace std;
using namespace kgmod;

// -----------------------------------------------------------------------------
// コンストラクタ(モジュール名，バージョン登録,パラメータ)
// -----------------------------------------------------------------------------
kgmod::kgCreGdb::kgCreGdb(void)
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

kgmod::kgCreGdb::~kgCreGdb(void) {
    if (occ != NULL) delete occ;
    if (config != NULL) delete config;
}

// -----------------------------------------------------------------------------
// 引数の設定
// -----------------------------------------------------------------------------
void kgmod::kgCreGdb::setArgs(void) {
    _args.paramcheck("i=,mp=,-d",kgArgs::COMMON|kgArgs::IODIFF|kgArgs::NULL_IN);
    
    opt_inf = _args.toString("i=", false);
    
    string wk_mp = _args.toString("mp=", false);
    if (wk_mp.size() != 0) opt_mp = stoi(wk_mp);
    
    opt_debug = _args.toBool("-d");
}

int kgmod::kgCreGdb::run() {
    try {
        setArgs();
        cerr << "reading config..." << endl;
        config = new Config(opt_inf);
        config->dump(opt_debug);
        if (! Cmn::MkDir(config->dbDir)) cerr << "failed to create dir " + config->dbDir << endl;
        
        occ = new Occ(config, _env);
        cerr << "creating gdb..." << endl;
        occ->build();
        cerr << "saving gdb..." << endl;
        occ->save(true);
        cerr << endl;
        
        occ->dump(opt_debug);
        
        cmdCache cmdcache(config, _env, true);
        cmdcache.dump(opt_debug);
        cmdcache.save();
        
        cerr << "done" << endl;
        
    } catch(kgError& err){
        errorEnd(err);
        return EXIT_FAILURE;
        
    } catch(char * er){
        kgError err(er);
        errorEnd(err);
        return EXIT_FAILURE;
    }
    
    return EXIT_SUCCESS;
}
