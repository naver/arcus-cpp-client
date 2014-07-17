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

#ifndef __DEF_ARCUS_LIST__
#define __DEF_ARCUS_LIST__


#include <list>

using namespace std;

#include "arcus.h"


struct arcusListInvalidIterator { };




template <typename T>
class arcusList
{
public:
	struct iterator/*{{{*/
	{
		typedef int difference_type;
		typedef T value_type;
		typedef T* pointer;
		typedef T& reference;
		typedef bidirectional_iterator_tag iterator_category;

		iterator(arcusList<T>* lst, int idx, const typename list<T>::iterator& it)
		{
			this->lst = lst;
			this->idx = idx;
			this->it = it;
			size = lst->size();
		}

		iterator& operator++()
		{
			++idx, ++it;
			return *this;
		}

		iterator operator++(int)
		{
			iterator old(lst, idx, it);
			++idx, ++it;
			return old;
		}

		iterator& operator--()
		{
			--idx, --it;
			return *this;
		}

		iterator operator--(int)
		{
			iterator old(lst, idx, it);
			--idx, --it;
			return old;
		}

		bool operator==(const iterator& rhs)
		{
			return (lst == rhs.lst &&
					idx == rhs.idx);
		}

		bool operator!=(const iterator& rhs)
		{
			return !(*this == rhs);
		}

		T operator*()
		{
			if (lst->on_cache == false) {
				throw arcusListInvalidIterator();
			}

			return *it;
		}

		int idx;
		int size;
		typename list<T>::iterator it;
		arcusList<T>* lst;
	};
/*}}}*/

	iterator begin()/*{{{*/
	{
		if (on_cache) {
			if (time(NULL) > this->next_refresh) {
				try {
					cache = client->lop_get(key)->get_result<list<T> >();
				}
				catch (arcusCollectionIndexException& e) {
					cache.clear();
				}
				next_refresh = time(NULL) + cache_time;
			}
		}

		return iterator(this, 0, cache.begin());
	}
/*}}}*/

	iterator end()/*{{{*/
	{
		if (on_cache) {
			if (time(NULL) > this->next_refresh) {
				try {
					cache = client->lop_get(key)->get_result<list<T> >();
				}
				catch (arcusCollectionIndexException& e) {
					cache.clear();
				}
				next_refresh = time(NULL) + cache_time;
			}
		}

		return iterator(this, size(), cache.end());
	}
/*}}}*/

public:
	arcusList(arcus* client, const string& key, int cache_time = 0)/*{{{*/
	{
		this->client = client;
		this->key = key;
		this->cache_time = cache_time;

		on_cache = false;
		if (cache_time > 0) {
			try {
				cache = client->lop_get(key)->get_result<list<T> >();
			}
			catch (arcusCollectionIndexException& e) {
				cache.clear();
			}
			on_cache = true;
		}

		next_refresh = time(NULL) + cache_time;
	}
/*}}}*/

	static arcusList<T> create(arcus* client, const string& key, ARCUS_TYPE type, int exptime = 0, int cache_time = 0)/*{{{*/
	{
		arcusFuture ft = client->lop_create(key, type, exptime);
		if (ft->get_result<bool>() == true) {
			arcusList<T> lst(client, key, cache_time);
			return lst;
		}

		throw arcusCollectionException();
	}
/*}}}*/

	static arcusList<T> get(arcus* client, const string& key, int cache_time = 0)/*{{{*/
	{
		arcusList<T> lst(client, key, cache_time);
		return lst;
	}
/*}}}*/

	size_t size()/*{{{*/
	{
		if (on_cache) {
			if (time(NULL) > next_refresh) {
				try {
					cache = client->lop_get(key)->get_result<list<T> >();
				}
				catch (arcusCollectionIndexException& e) {
					cache.clear();
				}

				next_refresh = time(NULL) + cache_time;
			}

			return cache.size();
		}

		try {
			return client->lop_get(key)->get_result<list<T> >().size();
		}
		catch (arcusCollectionIndexException& e) {
			return 0;
		}
	}
/*}}}*/

	bool push_front(const T& value)/*{{{*/
	{
		arcusFuture ft = client->lop_insert(key, 0, value);
		bool ret = ft->get_result<bool>();

		if (ret == true && on_cache) {
			if (time(NULL) > next_refresh) {
				try {
					cache = client->lop_get(key)->get_result<list<T> >();
				}
				catch (arcusCollectionIndexException& e) {
					cache.clear();
				}
				next_refresh = time(NULL) + cache_time;
			}
			else {
				cache.push_front(value);
			}
		}

		return ret;
	}
/*}}}*/

	bool pop_front()/*{{{*/
	{
		bool ret = client->lop_delete(key, 0, 0)->get_result<bool>();

		if (ret == true && on_cache) {
			if (time(NULL) > next_refresh) {
				try {
					cache = client->lop_get(key)->get_result<list<T> >();
				}
				catch (arcusCollectionIndexException& e) {
					cache.clear();
				}
				next_refresh = time(NULL) + cache_time;
			}
			else {
				cache.pop_front();
			}
		}

		return ret;
	}
/*}}}*/

	bool push_back(const T& value)/*{{{*/
	{
		arcusFuture ft = client->lop_insert(key, -1, value);
		bool ret = ft->get_result<bool>();

		if (ret == true && on_cache) {
			if (time(NULL) > next_refresh) {
				cache = client->lop_get(key)->get_result<list<T> >();
				next_refresh = time(NULL) + cache_time;
			}
			else {
				cache.push_back(value);
			}
		}

		return ret;
	}
/*}}}*/

	bool pop_back()/*{{{*/
	{
		bool ret = client->lop_delete(key, -1, -1)->get_result<bool>();

		if (ret == true && on_cache) {
			if (time(NULL) > next_refresh) {
				cache = client->lop_get(key)->get_result<list<T> >();
				next_refresh = time(NULL) + cache_time;
			}
			else {
				cache.pop_back();
			}
		}

		return ret;
	}
/*}}}*/

	bool erase(iterator first, iterator last)/*{{{*/
	{
		arcusFuture ft = client->lop_delete(key, first.idx, last.idx-1);
		bool ret = ft->get_result<bool>();

		if (ret == true && on_cache) {
			if (time(NULL) > next_refresh) {
				cache = client->lop_get(key)->get_result<list<T> >();
				next_refresh = time(NULL) + cache_time;
			}
			else {
				cache.erase(first.it, last.it);
			}
		}

		return ret;
	}
/*}}}*/

	template <class InputIterator>
	bool insert(iterator position, InputIterator first, InputIterator last)/*{{{*/
	{
		while (first != last) {
			arcusFuture ft = client->lop_insert(key, position.idx, *first);
			bool ret = ft->get_result<bool>();

			if (ret == false) {
				return false;
			}

			position.it = cache.insert(position.it, *first);

			++position;
			++first;

		}

		return true;
	}
/*}}}*/

	void invalidate()/*{{{*/
	{
		if (on_cache) {
			if (time(NULL) > this->next_refresh) {
				try {
					cache = client->lop_get(key)->get_result<list<T> >();
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
	list<T> cache;
	bool on_cache;
	time_t next_refresh;
};


#endif


