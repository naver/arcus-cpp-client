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

#ifndef __DEF_ARCUS_SET__
#define __DEF_ARCUS_SET__


#include <set>

using namespace std;

#include "arcus.h"


struct arcusSetInvalidIterator { };


template <typename T>
class arcusSet
{
public:
	struct iterator/*{{{*/
	{
		typedef int difference_type;
		typedef T value_type;
		typedef T* pointer;
		typedef T& reference;
		typedef bidirectional_iterator_tag iterator_category;

		iterator(arcusSet<T>* set_, const typename set<T>::iterator& it)
		{
			this->set_ = set_;
			this->it = it;
		}

		iterator& operator++()
		{
			++it;
			return *this;
		}

		iterator operator++(int)
		{
			iterator old(set_, it);
			++it;
			return old;
		}

		iterator& operator--()
		{
			--it;
			return *this;
		}

		iterator operator--(int)
		{
			iterator old(set_, it);
			--it;
			return old;
		}

		bool operator==(const iterator& rhs)
		{
			if (set_->on_cache == false) {
				throw arcusSetInvalidIterator();
			}

			return (set_ == rhs.set_ &&
					it == rhs.it);
		}

		bool operator!=(const iterator& rhs)
		{
			if (set_->on_cache == false) {
				throw arcusSetInvalidIterator();
			}

			return !(*this == rhs);
		}

		T operator*()
		{
			if (set_->on_cache == false) {
				throw arcusSetInvalidIterator();
			}

			return *it;
		}

		typename set<T>::iterator it;
		arcusSet<T>* set_;
	};
/*}}}*/

	iterator begin()/*{{{*/
	{
		if (on_cache) {
			if (time(NULL) > this->next_refresh) {
				try {
					cache = client->sop_get(key)->get_result<set<T> >();
				}
				catch (arcusCollectionIndexException& e) {
					cache.clear();
				}
				next_refresh = time(NULL) + cache_time;
			}
		}

		return iterator(this, cache.begin());
	}
/*}}}*/

	iterator end()/*{{{*/
	{
		if (on_cache) {
			if (time(NULL) > this->next_refresh) {
				try {
					cache = client->sop_get(key)->get_result<set<T> >();
				}
				catch (arcusCollectionIndexException& e) {
					cache.clear();
				}
				next_refresh = time(NULL) + cache_time;
			}
		}

		return iterator(this, cache.end());
	}
/*}}}*/

public:
	arcusSet(arcus* client, const string& key, int cache_time = 0)/*{{{*/
	{
		this->client = client;
		this->key = key;
		this->cache_time = cache_time;

		on_cache = false;
		if (cache_time > 0) {
			try {
				cache = client->sop_get(key)->get_result<set<T> >();
			}
			catch (arcusCollectionIndexException& e) {
				cache.clear();
			}
			on_cache = true;
		}

		next_refresh = time(NULL) + cache_time;
	}
/*}}}*/

	static arcusSet<T> create(arcus* client, const string& key, ARCUS_TYPE type, int exptime = 0, int cache_time = 0)/*{{{*/
	{
		arcusFuture ft = client->sop_create(key, type, exptime);
		if (ft->get_result<bool>() == true) {
			arcusSet<T> set_(client, key, cache_time);
			return set_;
		}

		throw arcusCollectionException();
	}
/*}}}*/

	static arcusSet<T> get(arcus* client, const string& key, int cache_time = 0)/*{{{*/
	{
		arcusSet<T> set_(client, key, cache_time);
		return set_;
	}
/*}}}*/

	size_t size()/*{{{*/
	{
		if (on_cache) {
			if (time(NULL) > next_refresh) {
				try {
					cache = client->sop_get(key)->get_result<set<T> >();
				}
				catch (arcusCollectionIndexException& e) {
					cache.clear();
				}

				next_refresh = time(NULL) + cache_time;
			}

			return cache.size();
		}

		try {
			return client->sop_get(key)->get_result<set<T> >().size();
		}
		catch (arcusCollectionIndexException& e) {
			return 0;
		}
	}
/*}}}*/

	iterator find(const T& value)/*{{{*/
	{
		if (on_cache) {
			if (time(NULL) > next_refresh) {
				try {
					cache = client->sop_get(key)->get_result<set<T> >();
				}
				catch (arcusCollectionIndexException& e) {
					cache.clear();
				}

				next_refresh = time(NULL) + cache_time;
			}

			typename set<T>::iterator it = cache.find(value);
			return iterator(this, it);
		}

		arcusFuture ft = client->sop_exist(key, value);
		bool ret = ft->get_result<bool>();

		if (true) {
			return iterator(this, cache.begin());
		}
		else {
			return iterator(this, cache.end());
		}
	}
/*}}}*/

	bool erase(iterator pos)/*{{{*/
	{
		return erase(*pos);
	}
/*}}}*/

	bool erase(const T& value)/*{{{*/
	{
		arcusFuture ft = client->sop_delete(key, value);
		bool ret = ft->get_result<bool>();

		if (ret == true && on_cache) {
			if (time(NULL) > next_refresh) {
				cache = client->sop_get(key)->get_result<set<T> >();
				next_refresh = time(NULL) + cache_time;
			}
			else {
				cache.erase(value);
			}
		}

		return ret;
	}
/*}}}*/

	template <class InputIterator>
	bool insert(InputIterator first, InputIterator last)/*{{{*/
	{
		while (first != last) {
			arcusFuture ft = client->sop_insert(key, *first);
			bool ret = ft->get_result<bool>();

			if (ret == false) {
				return false;
			}

			cache.insert(*first);
			++first;
		}

		return true;
	}
/*}}}*/

	bool insert(const T& value)/*{{{*/
	{
		arcusFuture ft = client->sop_insert(key, value);
		bool ret = ft->get_result<bool>();
		if (ret == false) {
			return false;
		}

		cache.insert(value);
		return true;
	}
/*}}}*/

	void invalidate()/*{{{*/
	{
		if (on_cache) {
			if (time(NULL) > this->next_refresh) {
				try {
					cache = client->sop_get(key)->get_result<set<T> >();
				}
				catch (arcusCollectionIndexException& e) {
					cache.clear();
				}
				next_refresh = time(NULL) + cache_time;
			}
		}
	}
/*}}}*/

protected:
	arcus* client;
	string key;
	int cache_time;
	set<T> cache;
	bool on_cache;
	time_t next_refresh;
};


#endif


