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
// =============================================================================
/// kgbindexHelp.h : kgbindex help
// =============================================================================
_title="-";
_doc="\
\n\
MINITBINDEX Extract records from a csv file with bitmap indexes\n\
\n\
Extract records from a csv file with bitmap indexes.\n\
\n\
Format\n\
\n\
mbindex I= e= [o=] [mp=] [--help] [--helpl] [--version]\n\
\n\
Parameters\n\
\n\
I=           ビットマップインデックスを作成するディレクトリ名を指定する。\n\
e=           検索条件式を指定する。\n\
o=           出力ファイル名（デフォルトは標準出力）\n\
             ファイル名に${項目名}を含めるとその項目の値毎にファイルを分割出力する。\n\
　　　　　　　　ただし、ここで指定する項目名に対してビットマップインデックスを作成しておく必要がある。\n\
             split=パラメータを指定して、ファイル名に${}を含めると0から始まる通番を採番する。\n\
             ${項目名}は同時に複数設定でき、${}と共存できる。\n\
split=       ここで指定した数だけCSVファイルを等分化して抽出処理をし、o=パラメータで指定したファイルに出力する。\n\
             o=パラメータの指定が必須となる。\n\
　　　　　　　　o=パラメータで指定するファイル名に${}が含まれていなければ、split=パラメータは無視される。\n\
　　　　　　　　mp=パラメータで指定した多重度で出力処理を行う。\n\
　　　　　　　　* なお、元のCSVファイルを等分して処理するため．出力ファイルが等分化される訳ではない。\n\
mp=          並列処理の多重度を指定する。（最大値はCPUのCORE数前後が目安）\n\
             ただし、split=パラメータを指定した場合のみ有効である。\n\
\n\
Specification of conditional expression\n\
\n\
  <colname>   ::= [ <text> ]+\n\
              |   '\"' [ <text> ]+ '\"'\n\
              |   \"'\" [ <text> ]+ \"'\"\n\
  <colnames>  ::= \"{\" <colname> [ \",\" <colname> ]* \"}\"\n\
  <value>     ::= [ <text> ]+\n\
              |   '\"' [ <text> ]* '\"'\n\
              |   \"'\" [ <text> ]* \"'\"\n\
              |   \"NULL\"\n\
  <interval>  ::= ( \"[\" | \"(\" ) <value> \",\" <value> ( \"]\" | \")\" )\n\
              |   ( \"[\" | \"(\" ) <value> \",\" ( \"]\" | \")\" )\n\
              |   ( \"[\" | \"(\" ) \",\" <value> ( \"]\" | \")\" )\n\
  <enum>      ::= \"{\" ( <value> | <interval> ) [ \",\" ( <value> | <interval> ) ]* \"}\"\n\
  <target>    ::= <value> | <enum>\n\
  <command>   ::= [ <text> ]+\n\
  <external>  ::= \"#{\" <command> \"}\"\n\
              |   \"`\" <command> \"`\"\n\
  <expr>      ::= <colname> \"=\" <target>\n\
              |   <colname> ( \"~\" | \"LIKE\" ) <value>\n\
              |   <colname> \"!=\" <value>\n\
              |   <colname> ( \"!~\" | \"NOT LIKE\" ) <value>\n\
              |   <colname> ( \">\" | \">=\" ) <value>\n\
              |   <colname> ( \"<\" | \"<=\" ) <value>\n\
              |   <colnames> \"=\" ( <external> | <enum> )\n\
  <factor>    ::= \"(\" <expr> | <not> | <minus> | <or> | <and> | <factor> \")\"\n\
  <not>       ::= ( \"^\" | \"!\" ) ( <expr> | <not> | <minus> | <or> | <and> | <factor> )\n\
  <minus>     ::= ( <expr> | <not> | <minus> | <or> | <and> | <factor>  \n\
                  \"-\" ( <expr> | <not> | <minus> | <or> | <and> | <factor> )\n\
  <and>       ::= ( <expr> | <not> | <minus> | <or> | <and> | <factor> ) \n\
                  \"&\" ( <expr> | <not> | <minus> | <or> | <and> | <factor> )\n\
  <or>        ::= ( <expr> | <not> | <minus> | <or> | <and> | <factor> ) \n\
                  \"|\" ( <expr> | <not> | <minus> | <or> | <and> | <factor> )\n\
\n\
Examples\n\
\n\
Example 1: 単一項目による抽出\n\
\n\
値は原則的にはシングルクォーテーション、シングルクォーテーションのいずれかで囲む。\n\
ただし、値にコンマやワイルドカード等の特殊文字が含まれない場合はクォーテーションで囲まなくてもよい。\n\
{値1,値2...}のように複数の値を同時に指定することができる。\n\
値にNULLを指定する時は，\"\"、''あるいは、NULL(大文字)を指定する。\n\
なおNULLを指定しない限り、該当項目がNULLとなるレコードは抽出しないので、\n\
e='key1={aaed,NULL}'など明示的に条件に指定する必要がある。\n\
また'!='や'!~'を用いた時、該当項目がNULLであるレコードは抽出しない。\n\
\n\
$ mbindex I=test.idx e='key1=aaed'\n\
$ mbindex I=test.idx e='key1=\"aaed\"'\n\
$ mbindex I=test.idx e='key1!=aaed'\n\
$ mbindex I=test.idx e='num1=NULL'\n\
$ mbindex I=test.idx e='num1=\"\"'\n\
$ mbindex I=test.idx e='num1={1,2}'\n\
$ mbindex I=test.idx e='num1!={1,2}'\n\
\n\
Example 2: 区間指定による抽出\n\
\n\
丸括弧は開区間、角括弧は閉区間を意味する。\n\
丸括弧と角括弧を組み合わせて使用することができる。\n\
'!='を用いた時，該当項目がNULLであるレコードは抽出しない。\n\
\n\
$ mbindex I=test.idx e='num3=[0.01,0.09)'\n\
$ mbindex I=test.idx e='num3=[0.99,)'\n\
$ mbindex I=test.idx e='num3=[,0.10)'\n\
$ mbindex I=test.idx e='num3!=[0.09,0.90)'\n\
\n\
Example 3: ワイルドカードによる抽出\n\
\n\
ワイルドカードには'?'(1文字)、'*'(1文字以上)を指定する。\n\
'NOT LIKE'を用いた時、該当項目がNULLであるレコードは抽出しない。\n\
\n\
$ mbindex I=test.idx e='key1~\"a*d\"'\n\
$ mbindex I=test.idx e='key1 LIKE \"a??d\"'\n\
$ mbindex I=test.idx e='key1!~\"*e*\"'\n\
$ mbindex I=test.idx e='key1 NOT LIKE \"a?e?\"'\n\
\n\
Example 4: 複数項目を列挙した抽出\n\
\n\
下の例ではe='key1=aaed | num1=3'と同じ結果が得られる。\n\
'!='を用いた時、該当項目がNULLであるレコードも抽出する。\n\
\n\
$ mbindex I=test.idx e='{key1,num1}={aaed,3}'\n\
$ mbindex I=test.idx e='{key1,num1}!={aaed,3}'\n\
\n\
Example 5: 外部コマンド(OSコマンド)の結果(ヘッダ付きCSV形式)を条件に抽出\n\
\n\
外部コマンド(OSコマンド)の出力結果はヘッダ付きCSV形式でなければならない。\n\
左辺で指定する項目名と一致する項目のデータと一致する項目の全レコード分の値を用いてレコード抽出する。\n\
複数項目列挙を用いることもできる。\n\
\n\
$ cat dim.csv\n\
id,key1,num1\n\
ID1,aaed,2\n\
ID2,aaef,4\n\
$ mbindex I=test.idx e='key1=`mcat i=dim.csv`'\n\
$ mbindex I=test.idx e='key1!=`mcat i=dim.csv`'\n\
$ mbindex I=test.idx e='{key1,num1}=#{mcat i=dim.csv}'\n\
$ mbindex I=test.idx e='{key1,num1}=${sh dim.sh}'\n\
\n\
Example 6: AND(&) / OR(|) / NOT(!,^) / MINUS(-) / ()\n\
\n\
'-'は左辺のレコード集合から右辺のレコード集合を除く演算子。\n\
式評価の優先順位は，(), NOT, MINUS, AND, ORの順。\n\
\n\
$ mbindex I=test.idx e='num1=1 | num1=2'\n\
$ mbindex I=test.idx e='num3>=0.01 & num3<0.09'\n\
$ mbindex I=test.idx e='!(key1={aaed,aaee})'\n\
$ mbindex I=test.idx e='^(key1={aaed,aaee})'\n\
$ mbindex I=test.idx e='key1=aaed - num1=2'\n\
$ mbindex I=datS.idx e='(key1=aaed | key1=aaee) - num1=0'\n\
\n\
Example 7: 出力先ファイル名の指定\n\
\n\
o=パラメータで出力先ファイル名を指定する。\n\
ファイル名に${項目名}を含めると該当項目の値毎にファイル分割する。\n\
*) ここで指定する項目名に対してビットマップインデックスを作成しておく必要がある。\n\
split=パラメータを指定して、ファイル名に${}を含めるとファイル分割する。\n\
*) 元のCSVファイルを等分して抽出処理するため．出力ファイルが等分化される訳ではない。\n\
${項目名}や${}を指定する場合，mp=パラメータによる並行処理が可能となる。\n\
\n\
$ mbindex I=test.idx e='num1=1' o=res.csv\n\
$ mbindex I=test.idx e='num1=1' o='res_${key1}.csv' mp=4\n\
$ mbindex I=test.idx e='num1=1' o='res_${}.csv' split=4 mp=4\n\
$ mbindex I-test.idx e='num1=1' o='res_${key1}_${key2}_${}.csv' split=4 mp=4\n\
";
