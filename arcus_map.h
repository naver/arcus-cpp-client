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

#ifndef __DEF_ARCUS_MAP__
#define __DEF_ARCUS_MAP__


#include <map>

using namespace std;

#include "arcus.h"


class arcusMapInvalidIterator {};
class arcusMapNotCached {};
class arcusMapException {};


template <typename K>
struct arcusBKeyMinMax
{
	static K get_min_key() { assert(false); }
	static K get_max_key() { assert(false); }
};


template <>
struct arcusBKeyMinMax<uint64_t>
{
	static uint64_t get_min_key()
	{ 
		return arcusBKey::get_min_long_key().l_key;
	}

	static uint64_t get_max_key()
	{ 
		return arcusBKey::get_max_long_key().l_key;
	}
};

template <>
struct arcusBKeyMinMax<vector<unsigned char> >
{
	static vector<unsigned char> get_min_key()
	{ 
		return arcusBKey::get_min_hex_key().h_key;
	}

	static vector<unsigned char> get_max_key()
	{ 
		return arcusBKey::get_max_hex_key().h_key;
	}
};




template <typename K, typename V>
class arcusMap
{
public:
	struct iterator/*{{{*/
	{
		typedef int difference_type;
		typedef pair<K, V> value_type;
		typedef pair<K, V>* pointer;
		typedef pair<K, V>& reference;
		typedef bidirectional_iterator_tag iterator_category;

		iterator(arcusMap<K, V>* map_, const K& bkey)
		{
			this->map_ = map_;
			this->bkey = bkey;
			this->on_cache = false;
		}

		iterator(arcusMap<K, V>* map_, const typename map<K, arcusBopItem<V> >::iterator& it)
		{
			this->map_ = map_;
			this->it = it;
			this->on_cache = true;
		}


		iterator& operator++()
		{
			if (on_cache) {
				++it;
			}
			else {
				throw arcusMapNotCached();
			}

			return *this;
		}

		iterator operator++(int)
		{
			if (on_cache) {
				iterator old(map_, it);
				++it;
				return old;
			}
			else {
				throw arcusMapNotCached();
			}
		}

		iterator& operator--()
		{
			if (on_cache) {
				--it;
			}
			else {
				throw arcusMapNotCached();
			}

			return *this;
		}

		iterator operator--(int)
		{
			if (on_cache) {
				iterator old(map_, it);
				--it;
				return old;
			}
			else {
				throw arcusMapNotCached();
			}
		}

		bool operator==(const iterator& rhs)
		{
			if (map_->on_cache) {
				return (map_ == rhs.map_ && it == rhs.it);
			}
			else {
				return (map_ == rhs.map_ && bkey == rhs.bkey);
			}
		}

		bool operator!=(const iterator& rhs)
		{
			return !(*this == rhs);
		}

		pair<K, arcusBopItem<V> > operator*()
		{
			if (map_->on_cache) {
				return pair<K, arcusBopItem<V> >(it->first, it->second);
			}

			return pair<K, arcusBopItem<V> >(bkey, arcusBopItem<V>());
		}

		K first()
		{
			if (map_->on_cache) {
				return it->first;
			}

			return bkey;
		}

		V second()
		{
			if (map_->on_cache == false) {
				throw arcusMapNotCached();
			}

			return it->second;
		}

		K bkey;
		typename map<K, arcusBopItem<V> >::iterator it;
		arcusMap<K, V>* map_;
		bool on_cache;
	};
/*}}}*/

	iterator begin()/*{{{*/
	{
		if (on_cache) {
			if (time(NULL) > this->next_refresh) {
				recache();
			}

			return iterator(this, cache.begin());
		}

		return iterator(this, arcusBKeyMinMax<K>::get_min_key());
	}
/*}}}*/

	iterator end()/*{{{*/
	{
		if (on_cache) {
			if (time(NULL) > this->next_refresh) {
				recache();
			}

			return iterator(this, cache.end());
		}

		return iterator(this, arcusBKeyMinMax<K>::get_max_key());
	}
/*}}}*/

	iterator lower_bound(const K& bkey)
	{
		if (on_cache) {
			return iterator(this, cache.lower_bound(bkey));
		}

		return iterator(this, bkey);
	}

public:
	arcusMap(arcus* client, const string& key, int cache_time = 0)/*{{{*/
	{
		this->client = client;
		this->key = key;
		this->cache_time = cache_time;

		on_cache = false;
		next_refresh = 0;
		if (cache_time > 0) {
			on_cache = true;
			recache();
		}
	}
/*}}}*/

	static arcusMap<K, V> create(arcus* client, const string& key, ARCUS_TYPE type, int exptime = 0, int cache_time = 0)/*{{{*/
	{
		arcusFuture ft = client->bop_create(key, type, exptime);
		if (ft->get_result<bool>() == true) {
			arcusMap<K, V> map_(client, key, cache_time);
			return map_;
		}

		throw arcusCollectionException();
	}
/*}}}*/

	static arcusMap<K, V> get(arcus* client, const string& key, int cache_time = 0)/*{{{*/
	{
		arcusMap<K, V> map_(client, key, cache_time);
		return map_;
	}
/*}}}*/

	size_t size()/*{{{*/
	{
		if (on_cache) {
			if (time(NULL) > next_refresh) {
				recache();
			}

			return cache.size();
		}

		try {
			arcusFuture ft = client->bop_get(key, arcusBKeyMinMax<K>::get_min_key(), arcusBKeyMinMax<K>::get_max_key());
			ft->get_result_map<K, V>().size();
		}
		catch (arcusCollectionIndexException& e) {
			return 0;
		}
	}
/*}}}*/

	bool erase(iterator first, iterator last)/*{{{*/
	{
		arcusFuture ft = client->bop_delete(key, first.first(), last.last());
		bool ret = ft->get_result<bool>();

		if (ret == true && on_cache) {
			if (time(NULL) > next_refresh) {
				recache();
			}
			else {
				cache.erase(first.it, last.it);
			}
		}

		return ret;
	}
/*}}}*/

	template <class InputIterator>
	bool insert(InputIterator first, InputIterator last)/*{{{*/
	{
		while (first != last) {
			arcusFuture ft = client->bop_insert(key, first->first, first->second.value, first->second.eflag);
			bool ret = ft->get_result<bool>();

			if (ret == false) {
				return false;
			}

			if (on_cache) {
				if (time(NULL) > this->next_refresh) {
					recache();
				}

				cache[first->first] = first->second;
			}

			++first;
		}

		return true;
	}
/*}}}*/

	bool insert(const K& bkey, const V& value, const arcusEflag& flag)/*{{{*/
	{
		arcusFuture ft = client->bop_insert(key, bkey, value, flag);
		bool ret = ft->get_result<bool>();

		if (ret == false) {
			return false;
		}

		if (on_cache) {
			if (time(NULL) > this->next_refresh) {
				recache();
			}

			cache[key] = arcusBopItem<V>(value, flag);
		}

		return true;
	}
/*}}}*/

	const arcusBopItem<V>& operator[](const K& bkey)/*{{{*/
	{
		if (on_cache) {
			if (time(NULL) > this->next_refresh) {
				recache();
			}

			return cache[bkey];
		}
		else {
			arcusFuture ft = client->bop_get(key, bkey, bkey);
			map<K, arcusBopItem<V> > ret = ft->get_result_map<K, V>();
			return ret[bkey];
		}
	}
/*}}}*/

	void invalidate()/*{{{*/
	{
		if (on_cache) {
			recache();
		}
	}
/*}}}*/


private:
	void recache()/*{{{*/
	{
		try {
			arcusFuture ft = client->bop_get(key, arcusBKeyMinMax<K>::get_min_key(), arcusBKeyMinMax<K>::get_max_key());
			cache = ft->get_result_map<K, V>();
		}
		catch (arcusCollectionIndexException& e) {
			cache.clear();
		}

		next_refresh = time(NULL) + cache_time;
	}
/*}}}*/

protected:
	arcus* client;
	string key;
	int cache_time;
	map<K, arcusBopItem<V> > cache;
	bool on_cache;
	time_t next_refresh;
};


#endif


