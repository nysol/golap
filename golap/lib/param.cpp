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

#include <boost/thread/mutex.hpp>
#include <chrono>
#include <unistd.h>
#include "param.hpp"
#include "cmn.hpp"

using namespace std;
using namespace kgmod;

boost::mutex Mtx;

kgmod::Param::Param(string& ParamFile) : ParamFile(ParamFile), flock(NULL) {
//    if (! ReadParam()) {
//        throw new Cmn::Exception("Failed to read file: " + ParamFile);
//    }
}

kgmod::Param::~Param(void) {
    if (flock != NULL) delete flock;
}

void kgmod::Param::FileLock(void) {
    if (! Cmn::FileExists(ParamFile)) return;
    if (flock == NULL) flock = new boost::interprocess::file_lock(ParamFile.c_str());
    flock->lock();
}

void kgmod::Param::FileUnlock(void) {
    if (! Cmn::FileExists(ParamFile)) return;
    if (flock == NULL) flock = new boost::interprocess::file_lock(ParamFile.c_str());
    flock->unlock();
}

bool kgmod::Param::ReadParam(void) {
    if (! Cmn::FileExists(ParamFile)) return true;
    try {
        boost::property_tree::read_json(ParamFile, pt);
        
    } catch (boost::property_tree::json_parser_error& e) {
        cerr << "#ERROR# ; " << e.what() << endl;
        cerr << "#ERROR# ; " << "Failed to read file: " << ParamFile << endl;
        return false;
    }
    return true;
}

bool kgmod::Param::WriteParam(void) {
    try {
        boost::property_tree::write_json(ParamFile, pt);
        
    } catch (boost::property_tree::json_parser_error& e) {
        cerr << "#ERROR# ; " << e.what() << endl;
        cerr << "#ERROR# ; " << "Failed to write file:" << ParamFile << endl;
        return false;
    }
    return true;
}