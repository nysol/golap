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

#include "kgcregdb.hpp"

using namespace std;
using namespace kgmod;

int main(int argc, const char* argv[]) {
    try {
        kgEnv    env;
        kgCreGdb kgmod;
        
        for(int i=1; i<argc; i++) {
            if(0==strcmp("--help",argv[i]) || 0==strcmp("-help",argv[i])) {
                cout << kgmod.doc() << endl;
                exit(EXIT_FAILURE);
            }
            if(0==strcmp("--helpl",argv[i]) || 0==strcmp("-helpl",argv[i])) {
                cout << kgmod.docl() << endl;
                exit(EXIT_FAILURE);
            }
            if(0==strcmp("--version",argv[i]) || 0==strcmp("-version",argv[i])) {
                cout << kgmod.lver() << endl;
                exit(EXIT_FAILURE);
            }
        }
        
        kgmod.init(argc, argv, &env);
        return kgmod.run();
        
    } catch(...) {
        return EXIT_FAILURE;
    }
}

