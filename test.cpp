/* vim: set fdc=2 foldmethod=marker ts=4 tabstop=4 sw=4 : */
/*
 * arcus-cpp-client - Arcus cpp client drvier
 * Copyright 2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 

#include <string>
#include <list>

using namespace std;

#include "arcus.h"
#include "arcus_mc_node.h"
#include "arcus_list.h"
#include "arcus_set.h"
#include "arcus_map.h"

template<typename T> void print(T value)/*{{{*/
{
	printf("## result: %s\n", lexical_cast<string>(value).c_str());
}
/*}}}*/

template<typename U> void print(list<U> value)/*{{{*/
{
	typename list<U>::iterator it;

	printf("## list: ");
	for (it = value.begin(); it != value.end(); ++it) {
		printf("%s, ", lexical_cast<string>(*it).c_str());
	}
	printf("\n");
}
/*}}}*/

template<typename U> void print(vector<U> value)/*{{{*/
{
	typename vector<U>::iterator it;

	printf("## vector: ");
	for (it = value.begin(); it != value.end(); ++it) {
		printf("%s, ", lexical_cast<string>(*it).c_str());
	}
	printf("\n");
}
/*}}}*/

template<typename U> void print(set<U> value)/*{{{*/
{
	typename set<U>::iterator it;

	printf("## set: ");
	for (it = value.begin(); it != value.end(); ++it) {
		printf("%s, ", lexical_cast<string>(*it).c_str());
	}
	printf("\n");
}
/*}}}*/

template<typename K, typename V> void print(map<K, arcusBopItem<V> > value)/*{{{*/
{
	typename map<K, arcusBopItem<V> >::iterator it;

	printf("## map: ");
	for (it = value.begin(); it != value.end(); ++it) {
		string key = lexical_cast<string>(it->first);
		printf("[%s:%s (%s)], ",
		key.c_str(),
		lexical_cast<string>(it->second.value).c_str(),
		it->second.eflag_hex_str().c_str());
	}
	printf("\n");
}
/*}}}*/

template<typename K, typename V> void print(map<string, map<K, arcusBopItem<V> > > value)/*{{{*/
{
	typename map<string, map<K, arcusBopItem<V> > >::iterator it_1;
	typename map<K, arcusBopItem<V> >::iterator it_2;

	printf("## mget: ");
	for (it_1 = value.begin(); it_1 != value.end(); ++it_1) {
		string key = lexical_cast<string>(it_1->first);

		for (it_2 = it_1->second.begin(); it_2 != it_1->second.end(); ++it_2) {
			string bkey = lexical_cast<string>(it_2->first);
			printf("[%s-%s:%s (%s)], ",
			key.c_str(),
			bkey.c_str(),
			lexical_cast<string>(it_2->second.value).c_str(),
			it_2->second.eflag_hex_str().c_str());
		}
	}
	printf("\n");
}
/*}}}*/

template<typename K, typename V> void print_vmap(map<K, arcusBopItem<V> > value)/*{{{*/
{
	typename map<K, arcusBopItem<V> >::iterator it;

	printf("## map: ");
	for (it = value.begin(); it != value.end(); ++it) {
		string key = util.vec2hstr(it->first);

		printf("[%s:%s (%s)], ",
		key.c_str(),
		lexical_cast<string>(it->second.value).c_str(),
		it->second.eflag_hex_str().c_str());
	}
	printf("\n");
}
/*}}}*/

template<typename K, typename V> void print(multimap<K, arcusBopItem<V> > value)/*{{{*/
{
	typename multimap<K, arcusBopItem<V> >::iterator it;

	printf("## multimap: ");
	for (it = value.begin(); it != value.end(); ++it) {
		string key = lexical_cast<string>(it->first);
		printf("[%s:%s (%s)], ",
		key.c_str(),
		lexical_cast<string>(it->second.value).c_str(),
		it->second.eflag_hex_str().c_str());
	}
	printf("\n");
}
/*}}}*/

template<typename K, typename V> void print_vmap(multimap<K, arcusBopItem<V> > value)/*{{{*/
{
	typename multimap<K, arcusBopItem<V> >::iterator it;

	printf("## multimap: ");
	for (it = value.begin(); it != value.end(); ++it) {
		string key = util.vec2hstr(it->first);

		printf("[%s:%s (%s)], ",
		key.c_str(),
		lexical_cast<string>(it->second.value).c_str(),
		it->second.eflag_hex_str().c_str());
	}
	printf("\n");
}
/*}}}*/


int main(int argc, char* argv[])
{
	int i, j, idx;
	if (argc < 3) {
		return 0;
	}

	arcusNodeAllocator* allocator = new arcusMCNodeAllocator(new arcusTranscoder());
	arcusLocator* locator =  new arcusLocator(allocator);
	arcus* client = new arcus(locator);

	client->connect(argv[1], argv[2]);

	arcus_log(NULL);

	arcusFuture ret;

	/***************************************************************************************************
	 *
	 * TEST 1: primitive type
	 *
	 ***************************************************************************************************/
	if (false) {
		ret = client->set("test:string1", "test...", 3);
		print(ret->get_result<bool>());
		assert (ret->get_result<bool>() == true);

		ret = client->get("test:string1");
		print(ret->get_result<string>());
		assert (ret->get_result<string>() == "test...");

		ret = client->set("test:string2", "test...2", 3);
		print(ret->get_result<bool>());
		assert (ret->get_result<bool>() == true);

		ret = client->get("test:string2");
		print(ret->get_result<string>());
		assert (ret->get_result<string>() == "test...2");

		ret = client->set("test:int", 1, 3);
		print(ret->get_result<bool>());
		assert (ret->get_result<bool>() == true);

		ret = client->get("test:int");
		print(ret->get_result<int>());
		assert (ret->get_result<int>() == 1);

		ret = client->set("test:float", 1.2, 3);
		print(ret->get_result<bool>());
		assert (ret->get_result<bool>() == true);

		ret = client->get("test:float");
		print(ret->get_result<double>());
		assert (ret->get_result<double>() == 1.2);

		ret = client->set("test:bool", true, 3);
		print(ret->get_result<bool>());
		assert (ret->get_result<bool>() == true);

		ret = client->get("test:bool");
		print(ret->get_result<bool>());
		assert (ret->get_result<bool>() == true);

		ptime now = microsec_clock::local_time();
		ret = client->set("test:date", now, 3);
		print(ret->get_result<bool>());
		assert (ret->get_result<bool>() == true);

		ret = client->get("test:date");
		print(ret->get_result<ptime>());
		print(now);
		assert ((ret->get_result<ptime>() - now) < millisec(1));

		vector<char> array;
		for (i=0; i<10; i++) array.push_back('a' + i);

		ret = client->set("test:bytearray", array, 3);
		print(ret->get_result<bool>());
		assert (ret->get_result<bool>() == true);

		ret = client->get("test:bytearray");
		vector<char> array_ret = ret->get_result<vector<char> >();
		print(array_ret);
		assert (ret->get_result<vector<char> >() == array);


		ret = client->set("test:incr", "1", 3);
		print(ret->get_result<bool>());
		assert (ret->get_result<bool>() == true);

		ret = client->incr("test:incr", 10);
		print(ret->get_result<int>());
		assert (ret->get_result<int>() == 11);

		ret = client->decr("test:incr", 3);
		print(ret->get_result<int>());
		assert (ret->get_result<int>() == 11-3);

		ret = client->decr("test:incr", 100);
		print(ret->get_result<int>());
		assert (ret->get_result<int>() == 0); // minimum value is 0
	}


	/***************************************************************************************************
	 *
	 * TEST 2: list
	 *
	 ***************************************************************************************************/
	if (false) {
		list<string> l_s_src;
		list<string> l_s;

		ret = client->lop_create("test:list_1", FLAG_STRING, 3);
		print(ret->get_result<bool>());
		assert (ret->get_result<bool>() == true);

		for (i=0; i<6; i++) {
			char buff[256];
			sprintf(buff, "item %d", i);
			l_s_src.push_back(buff);
		}

		for (list<string>::iterator it = l_s_src.begin(); it != l_s_src.end(); ++it) {
			ret = client->lop_insert("test:list_1", -1, *it);
			print(ret->get_result<bool>());
			assert (ret->get_result<bool>() == true);
		}

		ret = client->lop_get("test:list_1", 0, -1);
		l_s = ret->get_result<list<string> >();
		print(l_s);
		assert (l_s == l_s_src);

		ret = client->lop_get("test:list_1", 2, 4);
		l_s = ret->get_result<list<string> >();
		print(l_s);
		//assert (l_s == l_s_src[2:4+1]);

		ret = client->lop_get("test:list_1", 1, -2);
		l_s = ret->get_result<list<string> >();
		print(l_s);
		//assert (l_s == src[1:-2+1]);
	}



	/***************************************************************************************************
	 *
	 * TEST 3: set
	 *
	 ***************************************************************************************************/
	if (false) {
		set<string> s_s_src;
		set<string> s_s;

		ret = client->sop_create("test:set_1", FLAG_STRING, 3);
		print(ret->get_result<bool>());
		assert (ret->get_result<bool>() == true);

		for (i=0; i<6; i++) {
			char buff[256];
			sprintf(buff, "item %d", i);
			s_s_src.insert(buff);
		}

		for (set<string>::iterator it = s_s_src.begin();
		     it != s_s_src.end(); ++it)
		{
			ret = client->sop_insert("test:set_1", *it);
			print(ret->get_result<bool>());
			assert (ret->get_result<bool>() == true);
		}

		ret = client->sop_get("test:set_1");
		print(ret->get_result<set<string> >());
		assert (ret->get_result<set<string> >() == s_s_src);

		for (set<string>::iterator it = s_s_src.begin();
		     it != s_s_src.end(); ++it)
		{
			ret = client->sop_exist("test:set_1", *it);
			print(ret->get_result<bool>());
			assert (ret->get_result<bool>() == true);
		}


		ret = client->sop_exist("test:set_1", "item 100");
		print(ret->get_result<bool>());
		assert (ret->get_result<bool>() == false);
	}



	/***************************************************************************************************
	 *
	 * TEST 4: btree
	 *
	 ***************************************************************************************************/
	if (false) {
		map<uint64_t, arcusBopItem<int> > m_l_i;
		map<vector<unsigned char>, arcusBopItem<string> > m_v_s;

		// int key
		ret = client->bop_create("test:btree_int", FLAG_INTEGER, 3);
		print (ret->get_result<bool>());
		assert (ret->get_result<bool>() == true);



		for (i=0; i<1000; i++) {
			ret = client->bop_insert("test:btree_int", i, i, i);
			print(ret->get_result<bool>());
			assert (ret->get_result<bool>() == true);
		}

		ret = client->bop_get("test:btree_int", 200, 400);
		 m_l_i = ret->get_result_map<uint64_t, int>(); // It's also possible
		// m_l_i = ret->get_result<map<uint64_t, arcusBopItem<int> > >(); // this is also possible, but above will be more convnient.
		print (m_l_i);

		for (i=200; i<400; i++) {
			assert (m_l_i[i].eflag_uint64() == i);
			assert (m_l_i[i].value == i);
		}

		ret = client->bop_count("test:btree_int", 100, 199);
		print(ret->get_result<int>());
		assert (ret->get_result<int>() == 100);

		// hex key
		ret = client->bop_create("test:btree_hex", FLAG_STRING, 3);
		print (ret->get_result<bool>());
		assert (ret->get_result<bool>() == true);

		for (i=0; i<100; i++) {
			ret = client->bop_insert("test:btree_hex", util.i2vec(i),
					string("bop item ") + lexical_cast<string>(i), i);
			print(ret->get_result<bool>());
			assert (ret->get_result<bool>() == true);
		}

		ret = client->bop_get("test:btree_hex", util.i2vec(10), util.i2vec(30));
		m_v_s = ret->get_result_map<vector<unsigned char>, string>();
		print_vmap(m_v_s);

		for (i=10; i<30; i++) {
			assert (m_v_s[util.i2vec(i)].eflag_uint64() == i);
			assert (m_v_s[util.i2vec(i)].value == string("bop item ") + lexical_cast<string>(i));
		}


		// eflag test

		ret = client->bop_create("test:btree_eflag", FLAG_INTEGER, 3);
		print (ret->get_result<bool>());
		assert (ret->get_result<bool>() == true);

		for (i=0; i<1000; i++) {
			ret = client->bop_insert("test:btree_eflag", i, i, i);
			print(ret->get_result<bool>());
			assert (ret->get_result<bool>() == true);
		}

		ret = client->bop_get("test:btree_eflag", 200, 400, (arcusEflag() & 0x00ff) == 0x0001);
		m_l_i = ret->get_result_map<uint64_t, int>();
		print(m_l_i);
		assert (m_l_i[257].eflag_hex_str() == "0x0000000000000101");
		assert (m_l_i[257].value == 257);

		ret = client->bop_get("test:btree_eflag", 200, 400, (arcusEflag() & 0x00ff) > 0x0010);
		m_l_i = ret->get_result_map<uint64_t, int>();
		print(m_l_i);

		for (i=200; i<400; i++) {
			if ((i & 0x00ff) <= 0x0010) {
				if (m_l_i.find(i) != m_l_i.end()) {
					assert (false);
				}

				continue;
			}

			assert (m_l_i[i].eflag_uint64() == i);
			assert (m_l_i[i].value == i);
		}
	}


	/***************************************************************************************************
	 *
	 * TEST 5: btree mget, smget
	 *
	 ***************************************************************************************************/
	if (false) {
		multimap<uint64_t, arcusBopItem<int> > mm_l_i;
		map<string, map<uint64_t, arcusBopItem<int> > > mget_l_i;

		// int key
		ret = client->bop_create("test:btree_1", FLAG_INTEGER, 3);
		print (ret->get_result<bool>());
		assert (ret->get_result<bool>() == true);

		for (i=0; i<1000; i++) {
			ret = client->bop_insert("test:btree_1", i, i, i);
			print(ret->get_result<bool>());
			assert (ret->get_result<bool>() == true);
		}

		ret = client->bop_create("test:btree_2", FLAG_INTEGER, 3);
		print (ret->get_result<bool>());
		assert (ret->get_result<bool>() == true);

		for (i=1000; i<2000; i++) {
			ret = client->bop_insert("test:btree_2", i, i, i);
			print(ret->get_result<bool>());
			assert (ret->get_result<bool>() == true);
		}

		ret = client->bop_create("test:btree_3", FLAG_INTEGER, 3);
		print (ret->get_result<bool>());
		assert (ret->get_result<bool>() == true);

		for (i=2000; i<3000; i++) {
			ret = client->bop_insert("test:btree_3", i, i, i);
			print(ret->get_result<bool>());
			assert (ret->get_result<bool>() == true);
		}


		arcusFutureList rets;
		vector<string> key_list;


		key_list.clear();
		key_list.push_back("test:btree_1");
		key_list.push_back("test:btree_2");
		key_list.push_back("test:btree_3");
		key_list.push_back("test:btree_4");
		key_list.push_back("test:btree_5");
		rets = client->bop_mget(key_list, 500, 2500);
		mget_l_i = rets->get_mget_result<uint64_t, int>();
		print(mget_l_i);
		print(rets->get_missed_keys());
		assert(rets->get_missed_keys()[0] == "test:btree_4");
		assert(rets->get_missed_keys()[1] == "test:btree_5");



		key_list.clear();
		key_list.push_back("test:btree_1");
		key_list.push_back("test:btree_2");
		key_list.push_back("test:btree_3");
		key_list.push_back("test:btree_4");
		key_list.push_back("test:btree_5");
		rets = client->bop_smget(key_list, 500, 2500);
		mm_l_i = rets->get_smget_result<uint64_t, int>();
		print(mm_l_i);
		print(rets->get_missed_keys());

		idx = 500;
		for (multimap<uint64_t, arcusBopItem<int> >::iterator it = mm_l_i.begin(); 
		     it != mm_l_i.end(); ++it, idx++)
		{
			assert (it->first == idx);
			assert (it->second.value == idx);
			assert (it->second.eflag_uint64() == idx);
		}

		assert(rets->get_missed_keys()[0] == "test:btree_4");
		assert(rets->get_missed_keys()[1] == "test:btree_5");
	}



	/***************************************************************************************************
	 *
	 * TEST 6: dynamic list
	 *
	 ***************************************************************************************************/
	if (false) {
		list<string> l_s_src;
		for (i=0; i<6; i++) {
			char buff[256];
			sprintf(buff, "item %d", i);
			l_s_src.push_back(buff);
		}

		arcusList<string> arcus_list = arcusList<string>::create(client, "test:arcus_list", FLAG_STRING, 5, 3);
		assert (arcus_list.size() == 0);

		arcus_list.insert(arcus_list.begin(), l_s_src.begin(), l_s_src.end());
		assert (arcus_list.size() == l_s_src.size());

		arcusList<string>::iterator it_1 = arcus_list.begin();
		list<string>::iterator it_2 = l_s_src.begin();
		for (; it_1 != arcus_list.end(); ++it_1, ++it_2) {
			assert(*it_1 == *it_2);
		}
	}



	/***************************************************************************************************
	 *
	 * TEST 7: dynamic set
	 *
	 ***************************************************************************************************/
	if (false) {
		list<string> l_s_src;
		for (i=0; i<6; i++) {
			char buff[256];
			sprintf(buff, "item %d", i);
			l_s_src.push_back(buff);
		}

		arcusSet<string> arcus_set = arcusSet<string>::create(client, "test:arcus_set", FLAG_STRING, 5, 3);
		assert (arcus_set.size() == 0);

		arcus_set.insert(l_s_src.begin(), l_s_src.end());
		assert (arcus_set.size() == l_s_src.size());

		list<string>::iterator it = l_s_src.begin();
		for (; it != l_s_src.end(); ++it) {
			assert (arcus_set.find(*it) != arcus_set.end());
		}

		assert (arcus_set.find("invalid item") == arcus_set.end());

		arcusSet<string>::iterator it_1 = arcus_set.begin();
		for (; it_1 != arcus_set.end(); ++it_1) {
			print(*it_1);
		}
	}

	/***************************************************************************************************
	 *
	 * TEST 8: dynamic map
	 *
	 ***************************************************************************************************/
	if (true) {
		map<uint64_t, arcusBopItem<string> > m_is_src, new_map;
		for (i=0; i<6; i++) {
			char buff[256];
			sprintf(buff, "item %d", i);
			m_is_src[i] = arcusBopItem<string>(buff);
		}

		arcusMap<uint64_t, string> arcus_map = arcusMap<uint64_t, string>::create(client, "test:arcus_map", FLAG_STRING, 5, 3);
		assert (arcus_map.size() == 0);

		arcus_map.insert(m_is_src.begin(), m_is_src.end());
		assert (arcus_map.size() == m_is_src.size());

		new_map.insert(arcus_map.begin(), arcus_map.end());
		assert (arcus_map.size() == new_map.size());


		map<uint64_t, arcusBopItem<string> >::iterator it = m_is_src.begin();
		for (; it != m_is_src.end(); ++it) {
			assert (arcus_map[it->first].value == it->second.value);
		}
	}

	print ("### test done ###");

	client->disconnect();
}

	
