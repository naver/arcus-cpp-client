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
 

#ifndef __DEF_ARCUS__
#define __DEF_ARCUS__

// standard header
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <stdarg.h>

#include <string>
#include <vector>
#include <list>
#include <set>
#include <map>
#include <deque>

using namespace std;

// boost header
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/shared_ptr.hpp>

using namespace boost::gregorian;
using namespace boost::posix_time;

class arcusOperation;
typedef boost::shared_ptr<arcusOperation> arcusFuture;

class arcusOperationList;
typedef boost::shared_ptr<arcusOperationList> arcusFutureList;

class arcusNode;



// etc
#include <mhash.h>
#include "zookeeper.h"


#define MAX_ELEMENT_SIZE	4096
#define MAX_BKEY_LEN		31
#define MAX_EFLAG_LEN		31
#define MAX_CMD_LEN			1024



struct arcusUtil/*{{{*/
{
	arcusUtil()/*{{{*/
	{
		int i = 1000;
		int j = htonl(i);

		if (i != j) {
			little_endian = true;
		}
		else {
			little_endian = false;
		}
	}
/*}}}*/

	string vec2hstr(const vector<unsigned char>& in) const /*{{{*/
	{
		if (in.empty()) {
			return "";
		}

		char buff[1024];
		assert (in.size() < (sizeof(buff)/2 - 2));
		
		sprintf(buff, "0x");
		for (int i=0; i<in.size(); i++) {
			char tmp[32];
			sprintf(tmp, "%02x", in[i]);
			strcat(buff, tmp);
		}

		return buff;
	}
/*}}}*/

	int char2int(char in) const /*{{{*/
	{
		if (in >= '0' && in <= '9') {
			return in - '0';
		}
		else if (in >= 'A' && in <= 'F') {
			return in - 'A' + 10;
		}
		else if (in >= 'a' && in <= 'a') {
			return in - 'a' + 10;
		}

		return -1;
	}
/*}}}*/

	vector<unsigned char> hstr2vec(const string& in) const /*{{{*/
	{
		int i=0, size = in.size();;
		vector<unsigned char> ret;

		if (in[0] == '0' && in[1] == 'x') {
			i += 2; // skip prefix
		}

		for (; i<size - 1; i+=2) {
			int upper = char2int(in[i]);
			int lower = char2int(in[i+1]);

			if (upper < 0 || lower < 0) {
				// fail
				return vector<unsigned char>();
			}

			unsigned char c = upper*16 + lower;
			ret.push_back(c);
		}

		return ret;
	}
/*}}}*/

	int64_t htonll(int64_t x) const /*{{{*/
	{
		if (little_endian == false) {
			return x;
		}
 
		return ((((x) & 0xff00000000000000LL) >> 56) |
			(((x) & 0x00ff000000000000LL) >> 40) |
			(((x) & 0x0000ff0000000000LL) >> 24) |
			(((x) & 0x000000ff00000000LL) >> 8) |
			(((x) & 0x00000000ff000000LL) << 8) |
			(((x) & 0x0000000000ff0000LL) << 24) |
			(((x) & 0x000000000000ff00LL) << 40) |
			(((x) & 0x00000000000000ffLL) << 56));
	}
/*}}}*/

	int64_t ntohll(int64_t x) const /*{{{*/
	{
		if (little_endian == false) {
			return x;
		}
 
		return ((((x) & 0x00000000000000FF) << 56) |
			(((x) & 0x000000000000FF00) << 40) |
			(((x) & 0x0000000000FF0000) << 24) |
			(((x) & 0x00000000FF000000) << 8)  |
			(((x) & 0x000000FF00000000) >> 8)  |
			(((x) & 0x0000FF0000000000) >> 24) |
			(((x) & 0x00FF000000000000) >> 40) |
			(((x) & 0xFF00000000000000) >> 56));
	}
/*}}}*/

	vector<unsigned char> i2vec(uint64_t in) const /*{{{*/
	{
		uint64_t ul = htonll(in);

		vector<unsigned char> vec;
		vec.resize(sizeof(uint64_t));
		memcpy(&vec[0], &ul, sizeof(uint64_t));

		return vec;
	}
/*}}}*/

	vector<unsigned char> s2vec(const string& in) const /*{{{*/
	{
		vector<unsigned char> vec;
		copy(in.begin(), in.end(), back_inserter(vec));
		return vec;
	}
/*}}}*/


public:

	bool little_endian;
};
/*}}}*/

const static arcusUtil util;

//
// arcusException
////////////////////////////
/*{{{*/

enum exception_id
{
	EXCEPTION_NONE,
	ARCUS_EXCEPTION,
	ARCUS_PROTOCOL_EXCEPTION,
	ARCUS_TYPE_EXCEPTION,
	ARCUS_NODE_EXCEPTION,
	ARCUS_NODE_SOCKET_EXCEPTION,
	ARCUS_NODE_CONNECTION_EXCEPTION,
	ARCUS_COLLECTION_EXCEPTION,
	ARCUS_COLLECTION_TYPE_EXCEPTION,
	ARCUS_COLLECTION_EXISTS_EXCEPTION,
	ARCUS_COLLECTION_NOT_FOUND_EXCEPTION,
	ARCUS_COLLECTION_INDEX_EXCEPTION,
	ARCUS_COLLECTION_OVERFLOW_EXCEPTION,
	ARCUS_COLLECTION_UNREADABLE_EXCEPTION,
	ARCUS_COLLECTION_HEX_FORMAT_EXCEPTION,
	ARCUS_COLLECTION_FILTER_INVALID_EXCEPTION
};

class arcusException {};
class arcusProtocolException : public arcusException {};
class arcusTypeException : public arcusException {};

class arcusNodeException : public arcusException {};
class arcusNodeSocketException : public arcusNodeException {};
class arcusNodeConnectionException : public arcusNodeException {};

class arcusCollectionException : public arcusException {};
class arcusCollectionTypeException : public arcusCollectionException {};
class arcusCollectionExistsException : public arcusCollectionException {};
class arcusCollectionNotFoundException : public arcusCollectionException {};
class arcusCollectionIndexException : public arcusCollectionException {};
class arcusCollectionOverflowException : public arcusCollectionException {};
class arcusCollectionUnreadableException : public arcusCollectionException {};
class arcusCollectionHexFormatException : public arcusCollectionException {};
class arcusCollectionFilterInvalidException : public arcusCollectionException {};
/*}}}*/

enum ARCUS_TYPE/*{{{*/
{
	FLAG_MASK =	0xff00,

	FLAG_STRING	= 0,			// 0
	FLAG_BOOLEAN =(1<<8),		// 256
	FLAG_INTEGER = (2<<8),		// 512
	FLAG_LONG =	(3<<8),			// 768
	FLAG_DATE = (4<<8),			// 1024
	FLAG_BYTE =	(5<<8),			// 1280
	FLAG_FLOAT = (6<<8),		// 1536
	FLAG_DOUBLE = (7<<8),		// 1792
	FLAG_BYTEARRAY = (8<<8),	// 2048

	FLAG_SERIALIZED	 = 1,
	FLAG_COMPRESSED	 = 2,
	FLAG_PICKLE	 = 4,
};
/*}}}*/

#define TO_TYPE(a) ((ARCUS_TYPE)(a & FLAG_MASK))


enum COLLECTION_TYPE/*{{{*/
{
	COLL_NONE,
	COLL_LOP,
	COLL_SOP,
	COLL_BOP,
};
/*}}}*/

template <typename T> void arcus_log_template(const char* format, ...)/*{{{*/
{
	static bool enable = false;
	if (format == NULL) {
		enable = true;
		return;
	}

	if (enable == false) {
		return;
	}

	va_list ap;
	va_start(ap, format);
	vprintf(format, ap);
	va_end(ap);
}
/*}}}*/

#define arcus_log arcus_log_template<bool>

template <typename T> class async_queue/*{{{*/
{
public:
	async_queue()
	{
		pthread_mutex_init(&mutex, NULL);
		pthread_cond_init(&cond, NULL);
	}

	~async_queue()
	{
		pthread_mutex_destroy(&mutex);
		pthread_cond_destroy(&cond);
	}

    void push(T const& value)
	{
		pthread_mutex_lock(&mutex);
		data.push_front(value);
		pthread_mutex_unlock(&mutex);

		pthread_cond_signal(&cond);
    }

    T pop()
	{
		pthread_mutex_lock(&mutex);
		while (data.empty()) {
			pthread_cond_wait(&cond, &mutex);
		}

        T rc(data.back());
        data.pop_back();

		pthread_mutex_unlock(&mutex);
        return rc;
    }

	size_t size()
	{
		return data.size();
	}

private:
	pthread_mutex_t	mutex;
	pthread_cond_t 	cond;
	deque<T> 		data;
};
/*}}}*/

template <typename T> struct arcusBopItem/*{{{*/
{
	arcusBopItem() { }

	arcusBopItem(const T& v, const vector<unsigned char>& f, const string& k = "")
	{
		value = v;
		eflag = f;
		key = k;
	}

	arcusBopItem(const T& v, const string& s = "", const string& k = "")
	{
		value = v;
		eflag = util.s2vec(s);
		key = k;
	}

	string eflag_hex_str() 	// return eflag as "0x0011" format
	{
		return util.vec2hstr(eflag);
	}

	string eflag_str()	 // convert eflag as string
	{
		string str;
		copy(eflag.begin(), eflag.end(), back_inserter(str));

		return str;
	}

	vector<unsigned char> eflag_vector()	 // return eflag itself
	{
		return eflag;
	}

	uint64_t eflag_uint64()
	{
		if (eflag.size() > sizeof(uint64_t)) {
			throw arcusTypeException();
		}

		uint64_t ret;
		memcpy((void*)&ret, (const void*)&eflag[0], sizeof(uint64_t));
		ret = util.ntohll(ret);

		return ret;
	}



	T value;
	vector<unsigned char> eflag;
	string key;
};
/*}}}*/

union u_value/*{{{*/
{
	bool b;
	char c;
	int i;
	int64_t ll;
	float f;
	double d;

	list<bool> *l_b;
	list<char> *l_c;
	list<int> *l_i;
	list<int64_t> *l_ll;
	list<float> *l_f;
	list<double> *l_d;
	list<string> *l_s;
	list<ptime> *l_t;
	list<vector<char> > *l_array;

	set<bool> *s_b;
	set<char> *s_c;
	set<int> *s_i;
	set<int64_t> *s_ll;
	set<float> *s_f;
	set<double> *s_d;
	set<string> *s_s;
	set<ptime> *s_t;
	set<vector<char> > *s_array;

	// map 
	map<uint64_t, arcusBopItem<bool> > *m_l_b;
	map<uint64_t, arcusBopItem<char> > *m_l_c;
	map<uint64_t, arcusBopItem<int> > *m_l_i;
	map<uint64_t, arcusBopItem<int64_t> > *m_l_ll;
	map<uint64_t, arcusBopItem<float> > *m_l_f;
	map<uint64_t, arcusBopItem<double> > *m_l_d;
	map<uint64_t, arcusBopItem<string> > *m_l_s;
	map<uint64_t, arcusBopItem<ptime> > *m_l_t;
	map<uint64_t, arcusBopItem<vector<char> > > *m_l_array;

	map<vector<unsigned char>, arcusBopItem<bool> > *m_v_b;
	map<vector<unsigned char>, arcusBopItem<char> > *m_v_c;
	map<vector<unsigned char>, arcusBopItem<int> > *m_v_i;
	map<vector<unsigned char>, arcusBopItem<int64_t> > *m_v_ll;
	map<vector<unsigned char>, arcusBopItem<float> > *m_v_f;
	map<vector<unsigned char>, arcusBopItem<double> > *m_v_d;
	map<vector<unsigned char>, arcusBopItem<string> > *m_v_s;
	map<vector<unsigned char>, arcusBopItem<ptime> > *m_v_t;
	map<vector<unsigned char>, arcusBopItem<vector<char> > > *m_v_array;

	// smget 
	multimap<uint64_t, arcusBopItem<bool> > *mm_l_b;
	multimap<uint64_t, arcusBopItem<char> > *mm_l_c;
	multimap<uint64_t, arcusBopItem<int> > *mm_l_i;
	multimap<uint64_t, arcusBopItem<int64_t> > *mm_l_ll;
	multimap<uint64_t, arcusBopItem<float> > *mm_l_f;
	multimap<uint64_t, arcusBopItem<double> > *mm_l_d;
	multimap<uint64_t, arcusBopItem<string> > *mm_l_s;
	multimap<uint64_t, arcusBopItem<ptime> > *mm_l_t;
	multimap<uint64_t, arcusBopItem<vector<char> > > *mm_l_array;

	multimap<vector<unsigned char>, arcusBopItem<bool> > *mm_v_b;
	multimap<vector<unsigned char>, arcusBopItem<char> > *mm_v_c;
	multimap<vector<unsigned char>, arcusBopItem<int> > *mm_v_i;
	multimap<vector<unsigned char>, arcusBopItem<int64_t> > *mm_v_ll;
	multimap<vector<unsigned char>, arcusBopItem<float> > *mm_v_f;
	multimap<vector<unsigned char>, arcusBopItem<double> > *mm_v_d;
	multimap<vector<unsigned char>, arcusBopItem<string> > *mm_v_s;
	multimap<vector<unsigned char>, arcusBopItem<ptime> > *mm_v_t;
	multimap<vector<unsigned char>, arcusBopItem<vector<char> > > *mm_v_array;

	// for mget
	map<string, map<uint64_t, arcusBopItem<bool> > > *mget_l_b;
	map<string, map<uint64_t, arcusBopItem<char> > > *mget_l_c;
	map<string, map<uint64_t, arcusBopItem<int> > > *mget_l_i;
	map<string, map<uint64_t, arcusBopItem<int64_t> > > *mget_l_ll;
	map<string, map<uint64_t, arcusBopItem<float> > > *mget_l_f;
	map<string, map<uint64_t, arcusBopItem<double> > > *mget_l_d;
	map<string, map<uint64_t, arcusBopItem<string> > > *mget_l_s;
	map<string, map<uint64_t, arcusBopItem<ptime> > > *mget_l_t;
	map<string, map<uint64_t, arcusBopItem<vector<char> > > > *mget_l_array;

	map<string, map<vector<unsigned char>, arcusBopItem<bool> > > *mget_v_b;
	map<string, map<vector<unsigned char>, arcusBopItem<char> > > *mget_v_c;
	map<string, map<vector<unsigned char>, arcusBopItem<int> > > *mget_v_i;
	map<string, map<vector<unsigned char>, arcusBopItem<int64_t> > > *mget_v_ll;
	map<string, map<vector<unsigned char>, arcusBopItem<float> > > *mget_v_f;
	map<string, map<vector<unsigned char>, arcusBopItem<double> > > *mget_v_d;
	map<string, map<vector<unsigned char>, arcusBopItem<string> > > *mget_v_s;
	map<string, map<vector<unsigned char>, arcusBopItem<ptime> > > *mget_v_t;
	map<string, map<vector<unsigned char>, arcusBopItem<vector<char> > > > *mget_v_array;
};
	/*}}}*/

struct arcusValue/*{{{*/
{
	arcusValue()
	{
		cflags = COLL_NONE;
		flags = FLAG_INTEGER;
		u.i = 0;
		exception = EXCEPTION_NONE;
	}

	// primitive
	arcusValue(const int value)
	{
		cflags = COLL_NONE;
		flags = FLAG_INTEGER;
		u.i = value;
		exception = EXCEPTION_NONE;
	}

	arcusValue(const int64_t value)
	{
		cflags = COLL_NONE;
		flags = FLAG_LONG;
		u.ll = value;
		exception = EXCEPTION_NONE;
	}

	arcusValue(const char value)
	{
		cflags = COLL_NONE;
		flags = FLAG_BYTE;
		u.c = value;
		exception = EXCEPTION_NONE;
	}

	arcusValue(const char* value)
	{
		flags = FLAG_STRING;
		str = value;
		exception = EXCEPTION_NONE;
	}

	arcusValue(const string& value)
	{
		cflags = COLL_NONE;
		flags = FLAG_STRING;
		str = value;
		exception = EXCEPTION_NONE;
	}

	arcusValue(const ptime& value)
	{
		cflags = COLL_NONE;
		flags = FLAG_DATE;
		time = value;
		exception = EXCEPTION_NONE;
	}

	arcusValue(const vector<char>& value)
	{
		cflags = COLL_NONE;
		flags = FLAG_BYTEARRAY;
		vec = value;
		exception = EXCEPTION_NONE;
	}

	arcusValue(const bool value)
	{
		cflags = COLL_NONE;
		flags = FLAG_BOOLEAN;
		u.b = value;
		exception = EXCEPTION_NONE;
	}

	arcusValue(const float value)
	{
		cflags = COLL_NONE;
		flags = FLAG_FLOAT;
		u.f = value;
		exception = EXCEPTION_NONE;
	}

	arcusValue(const double value)
	{
		cflags = COLL_NONE;
		flags = FLAG_DOUBLE;
		u.d = value;
		exception = EXCEPTION_NONE;
	}

	arcusValue(const exception_id id)
	{
		exception = id;
	}

	string to_str()
	{
		char buff[1024];

		switch(cflags)
		{
		case COLL_LOP:
			return "<LOP>";

		case COLL_SOP:
			return "<SOP>";

		case COLL_BOP:
			return "<SOP>";

		case COLL_NONE:
		default:
			break;
		}

		switch(flags)
		{
		case FLAG_STRING:
			return str;
		case FLAG_BOOLEAN:
			return u.b ? "true" : "false";
		case FLAG_INTEGER:
			sprintf(buff, "%d", u.i);
			return buff;
		case FLAG_LONG:
			sprintf(buff, "%lld", u.ll);
			return buff;
		case FLAG_DATE:
		case FLAG_BYTE:
			sprintf(buff, "%c", u.c);
		case FLAG_FLOAT:
			sprintf(buff, "%f", u.f);
		case FLAG_DOUBLE:
			sprintf(buff, "%f", u.d);
		case FLAG_BYTEARRAY:
			break;
		}

		return "NULL";
	}

	ARCUS_TYPE flags;
	COLLECTION_TYPE cflags;

	u_value u;
	string str;
	ptime time;
	vector<char> vec;

	vector<string> missed_keys;
	exception_id exception;
};

/*}}}*/

struct arcusBKey/*{{{*/
{
	enum ARCUS_BKEY_TYPE
	{
		BKEY_LONG,
		BKEY_HEX,
	};

	arcusBKey()
	{
		bkey_type = BKEY_LONG;
		l_key = 0;
	}

	arcusBKey(const int value)
	{
		bkey_type = BKEY_LONG;
		l_key = value;
	}

	arcusBKey(const unsigned int value)
	{
		bkey_type = BKEY_LONG;
		l_key = value;
	}

	arcusBKey(const int64_t value)
	{
		bkey_type = BKEY_LONG;
		l_key = value;
	}

	arcusBKey(const uint64_t value)
	{
		bkey_type = BKEY_LONG;
		l_key = value;
	}

	arcusBKey(const string& value)
	{
		assert (value.size() <= MAX_BKEY_LEN);

		bkey_type = BKEY_HEX;
		copy(value.begin(), value.end(), back_inserter(h_key));
	}

	arcusBKey(const char value[], int len)
	{
		assert (len <= MAX_BKEY_LEN);

		bkey_type = BKEY_HEX;
		copy(value, value + len, back_inserter(h_key));
	}

	arcusBKey(const vector<unsigned char>& value)
	{
		assert (value.size() <= MAX_BKEY_LEN);

		bkey_type = BKEY_HEX;
		h_key = value;
	}

	string to_str() const
	{
		if (bkey_type == BKEY_LONG) {
			return lexical_cast<string>(l_key);
		}

		assert (h_key.size() <= MAX_BKEY_LEN);
		return util.vec2hstr(h_key);
	}

	static arcusBKey get_min_long_key()
	{
		return 0;
	}

	static arcusBKey get_max_long_key()
	{
		return std::numeric_limits<uint64_t>::max();
	}

	static arcusBKey get_min_hex_key()
	{
		vector<unsigned char> h_key;
		h_key.push_back(0);
		return h_key;
	}

	static arcusBKey get_max_hex_key()
	{
		vector<unsigned char> h_key;
		for (int i=0; i < MAX_BKEY_LEN; i++) {	
			h_key.push_back(0xff);
		}
		return h_key;
	}

	ARCUS_BKEY_TYPE bkey_type;
	uint64_t l_key;
	vector<unsigned char> h_key;
};
/*}}}*/

struct arcusEflag/*{{{*/
{
	arcusEflag() /*{{{*/
	{
		lhs_offset = -1;
	}
/*}}}*/

	arcusEflag(const uint64_t eflag) /*{{{*/
	{
		lhs_offset = -1;
		this->eflag = util.vec2hstr(util.i2vec(eflag));
	}
/*}}}*/

	arcusEflag(const string& eflag) /*{{{*/
	{
		lhs_offset = -1;
		this->eflag = util.vec2hstr(util.s2vec(eflag));
	}
/*}}}*/

	arcusEflag(const vector<unsigned char>& eflag) /*{{{*/
	{
		lhs_offset = -1;
		this->eflag = util.vec2hstr(eflag);
	}
/*}}}*/

	arcusEflag(const arcusEflag& rhs)/*{{{*/
	{
		eflag = rhs.eflag;
		lhs_offset = rhs.lhs_offset;
		bit_op = rhs.bit_op;
		bit_rhs = rhs.bit_rhs;
		comp_op = rhs.comp_op;
		comp_rhs = rhs.comp_rhs;
	}
/*}}}*/

	arcusEflag& operator=(const arcusEflag& rhs)/*{{{*/
	{
		eflag = rhs.eflag;
		lhs_offset = rhs.lhs_offset;
		bit_op = rhs.bit_op;
		bit_rhs = rhs.bit_rhs;
		comp_op = rhs.comp_op;
		comp_rhs = rhs.comp_rhs;

		return *this;
	}
/*}}}*/

	string to_str() const /*{{{*/
	{
		if (eflag != "") {
			return eflag;
		}

		string expr = "";
		if (lhs_offset >= 0) {
			expr += lexical_cast<string>(lhs_offset);
			
			if (bit_op != "" && bit_rhs != "") {
				expr += " " + bit_op + " " + bit_rhs;
			}
		
			if (comp_op != "" && comp_rhs != "") {
				expr += " " + comp_op + " " + comp_rhs;
			}
			else {
				throw arcusCollectionFilterInvalidException();
			}
		}

		return expr;
	}
/*}}}*/



// shift operator
	arcusEflag operator<<(int offset)/*{{{*/
	{
		lhs_offset = offset;
		return *this;
	}
/*}}}*/


// bit-op with uint64_t rhs
	arcusEflag operator&(const uint64_t rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		bit_op = "&";
		bit_rhs = util.vec2hstr(util.i2vec(rhs));
		return *this;
	}
/*}}}*/

	arcusEflag operator|(const uint64_t rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		bit_op = "|";
		bit_rhs = util.vec2hstr(util.i2vec(rhs));
		return *this;
	}
/*}}}*/

	arcusEflag operator<(const uint64_t rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		comp_op = "LT";
		comp_rhs = util.vec2hstr(util.i2vec(rhs));
		return *this;
	}
/*}}}*/

	arcusEflag operator<=(const uint64_t rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		comp_op = "LE";
		comp_rhs = util.vec2hstr(util.i2vec(rhs));
		return *this;
	}
/*}}}*/
	
	arcusEflag operator>(const uint64_t rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		comp_op = "GT";
		comp_rhs = util.vec2hstr(util.i2vec(rhs));
		return *this;
	}
/*}}}*/

	arcusEflag operator>=(const uint64_t rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		comp_op = "GE";
		comp_rhs = util.vec2hstr(util.i2vec(rhs));
		return *this;
	}
/*}}}*/

	arcusEflag operator==(const uint64_t rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		comp_op = "EQ";
		comp_rhs = util.vec2hstr(util.i2vec(rhs));
		return *this;
	}
/*}}}*/

	arcusEflag operator!=(const uint64_t rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		comp_op = "NE";
		comp_rhs = util.vec2hstr(util.i2vec(rhs));
		return *this;
	}
/*}}}*/


// bit-op with string rhs
	arcusEflag operator&(const string& rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		bit_op = "&";
		bit_rhs = util.vec2hstr(util.s2vec(rhs));
		return *this;
	}
/*}}}*/

	arcusEflag operator|(const string& rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		bit_op = "|";
		bit_rhs = util.vec2hstr(util.s2vec(rhs));
		return *this;
	}
/*}}}*/

	arcusEflag operator<(const string& rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		comp_op = "<";
		comp_rhs = util.vec2hstr(util.s2vec(rhs));
		return *this;
	}
/*}}}*/

	arcusEflag operator<=(const string& rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		comp_op = "<=";
		comp_rhs = util.vec2hstr(util.s2vec(rhs));
		return *this;
	}
/*}}}*/
	
	arcusEflag operator>(const string& rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		comp_op = ">";
		comp_rhs = util.vec2hstr(util.s2vec(rhs));
		return *this;
	}
/*}}}*/

	arcusEflag operator>=(const string& rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		comp_op = ">=";
		comp_rhs = util.vec2hstr(util.s2vec(rhs));
		return *this;
	}
/*}}}*/

	arcusEflag operator==(const string& rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		comp_op = "==";
		comp_rhs = util.vec2hstr(util.s2vec(rhs));
		return *this;
	}
/*}}}*/

	arcusEflag operator!=(const string& rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		comp_op = "!=";
		comp_rhs = util.vec2hstr(util.s2vec(rhs));
		return *this;
	}
/*}}}*/


// bit-op with vector rhs
	arcusEflag operator&(const vector<unsigned char>& rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		bit_op = "&";
		bit_rhs = util.vec2hstr(rhs);
		return *this;
	}
/*}}}*/

	arcusEflag operator|(const vector<unsigned char>& rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		bit_op = "|";
		bit_rhs = util.vec2hstr(rhs);
		return *this;
	}
/*}}}*/

	arcusEflag operator^(const vector<unsigned char>& rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		bit_op = "~";
		bit_rhs = util.vec2hstr(rhs);
		return *this;
	}
/*}}}*/

	arcusEflag operator<(const vector<unsigned char>& rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		comp_op = "<";
		comp_rhs = util.vec2hstr(rhs);
		return *this;
	}
/*}}}*/

	arcusEflag operator<=(const vector<unsigned char>& rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		comp_op = "<=";
		comp_rhs = util.vec2hstr(rhs);
		return *this;
	}
/*}}}*/
	
	arcusEflag operator>(const vector<unsigned char>& rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		comp_op = ">";
		comp_rhs = util.vec2hstr(rhs);
		return *this;
	}
/*}}}*/

	arcusEflag operator>=(const vector<unsigned char>& rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		comp_op = ">=";
		comp_rhs = util.vec2hstr(rhs);
		return *this;
	}
/*}}}*/

	arcusEflag operator==(const vector<unsigned char>& rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		comp_op = "==";
		comp_rhs = util.vec2hstr(rhs);
		return *this;
	}
/*}}}*/

	arcusEflag operator!=(const vector<unsigned char>& rhs)/*{{{*/
	{
		if (lhs_offset < 0) lhs_offset = 0;
		comp_op = "!=";
		comp_rhs = util.vec2hstr(rhs);
		return *this;
	}
/*}}}*/

	string eflag;
	int lhs_offset;

	string bit_op;
	string bit_rhs;

	string comp_op;
	string comp_rhs;
};
/*}}}*/

const static arcusEflag eflag_none;
const static arcusEflag default_filter;

struct arcusData/*{{{*/
{
	arcusData() {}

	arcusData(ARCUS_TYPE f, char s[], int len)
	{
		flags = f;
		copy(s, s+len, back_inserter(value));
	}

	arcusData(ARCUS_TYPE f, const string& s)
	{
		flags = f;
		copy(s.begin(), s.end(), back_inserter(value));
	}

	arcusData(ARCUS_TYPE f, const vector<char>& s)
	{
		flags = f;
		copy(s.begin(), s.end(), back_inserter(value));
	}

	int flags;
	vector<char> value;
};
/*}}}*/

struct arcusAttribute/*{{{*/
{
	arcusAttribute()
	{
		// for create on insert
		create = false;
		flags = 0;
		exptime = 0;

		// default value
		max_count = 4000;
		ovfl_action = OVFL_TAIL_TRIM;
		readable = true;
		noreply = false;

		// additional parameters (not real attributes)
		del = false;
		drop = false;
		pipe = false;
	}

	enum OVFL_ACTION
	{
		OVFL_TAIL_TRIM,
		OVFL_HEAD_TRIM,
		OVFL_ERROR,
	};

	const char* get_ovfl_action() const
	{
		switch (ovfl_action)
		{
		case OVFL_TAIL_TRIM:
			return "";
		case OVFL_HEAD_TRIM:
			return " head_trim";
		case OVFL_ERROR:
			return " error";
		}

		return "";
	}

	int flags;
	int exptime;
	int max_count;
	OVFL_ACTION ovfl_action;
	bool readable;
	bool noreply;

	bool pipe;
	bool create;
	bool del;
	bool drop;
};
/*}}}*/

const static arcusAttribute default_attrs;



class arcusTranscoder/*{{{*/
{
public:
	arcusTranscoder() { }

	virtual ~arcusTranscoder() {}

	virtual arcusData encode(const arcusValue& v)/*{{{*/
	{
		switch(v.flags & FLAG_MASK)
		{
		case FLAG_STRING: {
			return arcusData(FLAG_STRING, v.str);
			}

		case FLAG_BOOLEAN: {
			char s[1];
			s[0] = v.u.b?1:0;
			return arcusData(FLAG_BOOLEAN, s, 1);
			}

		case FLAG_INTEGER: {
			int i = htonl(v.u.i);
			return arcusData(FLAG_INTEGER, (char*)&i, sizeof(int));
			}

		case FLAG_LONG: {
			int64_t ll = util.htonll(v.u.ll);
			return arcusData(FLAG_LONG, (char*)&ll, sizeof(int64_t));
			}

		case FLAG_DATE: {
			int64_t ll = (v.time - ptime(date(1970, Jan, 1))).total_milliseconds();
			ll = util.htonll(ll);
			return arcusData(FLAG_DATE, (char*)&ll, sizeof(int64_t));
			}

		case FLAG_BYTE: {
			char s[1];
			s[0] = v.u.c;
			return arcusData(FLAG_BYTE, s, 1);
			}

		case FLAG_FLOAT: {
			assert (sizeof(int) == sizeof(float));

			int i = htonl(*(int*)&v.u.f);
			return arcusData(FLAG_FLOAT, (char*)&i, sizeof(int));
			}

		case FLAG_DOUBLE: {
			assert (sizeof(int64_t) == sizeof(double));

			int64_t i = util.htonll(*(int64_t*)&v.u.d);
			return arcusData(FLAG_DOUBLE, (char*)&i, sizeof(int64_t));
			}

		case FLAG_BYTEARRAY: {
			return arcusData(FLAG_BYTEARRAY, v.vec);
			}
		}

		// dummy
		assert (false);
		return arcusData(FLAG_STRING, "");
	}
/*}}}*/

	virtual arcusValue decode(ARCUS_TYPE flags, char* data, int len)/*{{{*/
	{
		switch(flags & FLAG_MASK)
		{
		case FLAG_STRING: {
			return data;
		}

		case FLAG_BOOLEAN: {
			return data[0] == 1 ? true : false;
		}

		case FLAG_INTEGER: {
			int *i = (int*)data;
			int j = ntohl(*i);

			return j;
		}

		case FLAG_LONG: {
			int64_t *i = (int64_t*)data;
			int64_t j = util.ntohll(*i);

			return j;
		}

		case FLAG_DATE: {
			int64_t *i = (int64_t*)data;
			int64_t j = util.ntohll(*i);

			ptime time = from_time_t(j/1000) + millisec(j % 1000);
			return time;
		}

		case FLAG_BYTE: {
			return data[0];
		}

		case FLAG_FLOAT: {
			float j;
			int *i = (int*)data;

			*(int*)&j = ntohl(*i);

			return j;
		}

		case FLAG_DOUBLE: {
			double j;
			int64_t *i = (int64_t*)data;

			*(int64_t*)&j = util.ntohll(*i);

			return j;
		}
		case FLAG_BYTEARRAY: {
			vector<char> v;
			copy(data, data + len, back_inserter(v));
			return v;
			}
		}

		// dummy
		assert (false);
		return false;
	}/*}}}*/
};
/*}}}*/


class arcusHash/*{{{*/
{
public:
	virtual ~arcusHash() {};
	virtual vector<unsigned int> hash(const string& addr) = 0;
};
/*}}}*/

class arcusKetemaHash : public arcusHash/*{{{*/
{
public:
	arcusKetemaHash(int per_node=20, int per_hash=4)
	{
		this->per_node = per_node;
		this->per_hash = per_hash;
	}

	virtual vector<unsigned int> hash(const string& addr)
	{
		vector<unsigned int> ret;

		for (int i=0; i<per_node; i++) {
			char buff[1024];
			snprintf(buff, sizeof(buff), "%s-%d", addr.c_str(), i);
			hash_node(buff, ret);
		}

		return ret;
	}
	

private:
	void hash_node(const string& input, vector<unsigned int>& ret)
	{
		unsigned int hash;
		unsigned char r[16];

		MHASH td = mhash_init(MHASH_MD5);
		mhash(td, (unsigned char*)input.c_str(), input.length());
		mhash_deinit(td, r);

		for (int i=0; i<per_hash; i++) {
			hash = (r[3 + i*4] << 24) | (r[2 + i*4] << 16) | (r[1 + i*4] << 8) | r[0 + i*4];
			ret.push_back(hash);
		}
	}

	int per_node;
	int per_hash;
};
/*}}}*/


//
// arcusValueType templates
//////////////////////// 
/*{{{*/

template <typename T>
struct arcusValueType
{
	static T decode(const arcusValue& v)
	{
		assert (false);
	}
};

//
// primitive
////////////////////
template <> struct arcusValueType<string>/*{{{*//*{{{*/
{
	static string decode(const arcusValue& v)
	{
		if (v.cflags != COLL_NONE) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_STRING) {
			throw arcusTypeException();
		}

		return v.str;
	}
};
/*}}}*/

template <> struct arcusValueType<ptime>/*{{{*/
{
	static ptime decode(const arcusValue& v)
	{
		if (v.cflags != COLL_NONE) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_DATE) {
			throw arcusTypeException();
		}

		return v.time;
	}
};
/*}}}*/

template <> struct arcusValueType<vector<char> >/*{{{*/
{
	static vector<char> decode(const arcusValue& v)
	{
		if (v.cflags != COLL_NONE) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BYTEARRAY) {
			throw arcusTypeException();
		}

		return v.vec;
	}
};
/*}}}*/

template <> struct arcusValueType<bool>/*{{{*/
{
	static bool decode(const arcusValue& v)
	{
		if (v.cflags != COLL_NONE) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BOOLEAN) {
			throw arcusTypeException();
		}

		return v.u.b;
	}
};
/*}}}*/

template <> struct arcusValueType<char>/*{{{*/
{
	static char decode(const arcusValue& v)
	{
		if (v.cflags != COLL_NONE) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BYTE) {
			throw arcusTypeException();
		}

		return v.u.c;
	}
};

/*}}}*/

template <> struct arcusValueType<int>/*{{{*/
{
	static int decode(const arcusValue& v)
	{
		if (v.cflags != COLL_NONE) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_INTEGER) {
			throw arcusTypeException();
		}

		return v.u.i;
	}
};
/*}}}*/

template <> struct arcusValueType<int64_t>/*{{{*/
{
	static int64_t decode(const arcusValue& v)
	{
		if (v.cflags != COLL_NONE) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_LONG) {
			throw arcusTypeException();
		}

		return v.u.ll;
	}
};
/*}}}*/

template <> struct arcusValueType<float>/*{{{*/
{
	static float decode(const arcusValue& v)
	{
		if (v.cflags != COLL_NONE) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_FLOAT) {
			throw arcusTypeException();
		}

		return v.u.f;
	}
};
/*}}}*/

template <> struct arcusValueType<double>/*{{{*/
{
	static double decode(const arcusValue& v)
	{
		if (v.cflags != COLL_NONE) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_DOUBLE) {
			throw arcusTypeException();
		}

		return v.u.d;
	}
};
/*}}}*/
/*}}}*/

//
// lop
////////////////////
template <> struct arcusValueType<list<string>* >/*{{{*//*{{{*/
{
	static list<string>* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_LOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_STRING) {
			throw arcusTypeException();
		}

		return v.u.l_s;
	}
};
/*}}}*/
template <> struct arcusValueType<list<string> >/*{{{*/
{
	static list<string> decode(const arcusValue& v)
	{
		return *arcusValueType<list<string>* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<list<ptime>* >/*{{{*/
{
	static list<ptime>* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_LOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_DATE) {
			throw arcusTypeException();
		}

		return v.u.l_t;
	}
};
/*}}}*/
template <> struct arcusValueType<list<ptime> >/*{{{*/
{
	static list<ptime> decode(const arcusValue& v)
	{
		return *arcusValueType<list<ptime>* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<list<vector<char> >* >/*{{{*/
{
	static list<vector<char> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_LOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BYTEARRAY) {
			throw arcusTypeException();
		}

		return v.u.l_array;
	}
};
/*}}}*/
template <> struct arcusValueType<list<vector<char> > >/*{{{*/
{
	static list<vector<char> > decode(const arcusValue& v)
	{
		return *arcusValueType<list<vector<char> >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<list<bool>* >/*{{{*/
{
	static list<bool>* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_LOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BOOLEAN) {
			throw arcusTypeException();
		}

		return v.u.l_b;
	}
};
/*}}}*/
template <> struct arcusValueType<list<bool> >/*{{{*/
{
	static list<bool> decode(const arcusValue& v)
	{
		return *arcusValueType<list<bool>* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<list<char>* >/*{{{*/
{
	static list<char>* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_LOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BYTE) {
			throw arcusTypeException();
		}

		return v.u.l_c;
	}
};
/*}}}*/
template <> struct arcusValueType<list<char> >/*{{{*/
{
	static list<char> decode(const arcusValue& v)
	{
		return *arcusValueType<list<char>* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<list<int>* >/*{{{*/
{
	static list<int>* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_LOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_INTEGER) {
			throw arcusTypeException();
		}

		return v.u.l_i;
	}
};
/*}}}*/
template <> struct arcusValueType<list<int> >/*{{{*/
{
	static list<int> decode(const arcusValue& v)
	{
		return *arcusValueType<list<int>* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<list<int64_t>* >/*{{{*/
{
	static list<int64_t>* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_LOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_LONG) {
			throw arcusTypeException();
		}

		return v.u.l_ll;
	}
};
/*}}}*/
template <> struct arcusValueType<list<int64_t> >/*{{{*/
{
	static list<int64_t> decode(const arcusValue& v)
	{
		return *arcusValueType<list<int64_t>* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<list<float>* >/*{{{*/
{
	static list<float>* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_LOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_FLOAT) {
			throw arcusTypeException();
		}

		return v.u.l_f;
	}
};
/*}}}*/
template <> struct arcusValueType<list<float> >/*{{{*/
{
	static list<float> decode(const arcusValue& v)
	{
		return *arcusValueType<list<float>* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<list<double>* >/*{{{*/
{
	static list<double>* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_LOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_DOUBLE) {
			throw arcusTypeException();
		}

		return v.u.l_d;
	}
};
/*}}}*/
template <> struct arcusValueType<list<double> >/*{{{*/
{
	static list<double> decode(const arcusValue& v)
	{
		return *arcusValueType<list<double>* >::decode(v);
	}
};
/*}}}*/
/*}}}*/

//
// set
////////////////////
template <> struct arcusValueType<set<string>* >/*{{{*//*{{{*/
{
	static set<string>* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_SOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_STRING) {
			throw arcusTypeException();
		}

		return v.u.s_s;
	}
};
/*}}}*/
template <> struct arcusValueType<set<string> >/*{{{*/
{
	static set<string> decode(const arcusValue& v)
	{
		return *arcusValueType<set<string>* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<set<ptime>* >/*{{{*/
{
	static set<ptime>* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_SOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_DATE) {
			throw arcusTypeException();
		}

		return v.u.s_t;
	}
};
/*}}}*/
template <> struct arcusValueType<set<ptime> >/*{{{*/
{
	static set<ptime> decode(const arcusValue& v)
	{
		return *arcusValueType<set<ptime>* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<set<vector<char> >* >/*{{{*/
{
	static set<vector<char> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_SOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BYTEARRAY) {
			throw arcusTypeException();
		}

		return v.u.s_array;
	}
};
/*}}}*/
template <> struct arcusValueType<set<vector<char> > >/*{{{*/
{
	static set<vector<char> > decode(const arcusValue& v)
	{
		return *arcusValueType<set<vector<char> >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<set<bool>* >/*{{{*/
{
	static set<bool>* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_SOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BOOLEAN) {
			throw arcusTypeException();
		}

		return v.u.s_b;
	}
};
/*}}}*/
template <> struct arcusValueType<set<bool> >/*{{{*/
{
	static set<bool> decode(const arcusValue& v)
	{
		return *arcusValueType<set<bool>* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<set<char>* >/*{{{*/
{
	static set<char>* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_SOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BYTE) {
			throw arcusTypeException();
		}

		return v.u.s_c;
	}
};
/*}}}*/
template <> struct arcusValueType<set<char> >/*{{{*/
{
	static set<char> decode(const arcusValue& v)
	{
		return *arcusValueType<set<char>* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<set<int>* >/*{{{*/
{
	static set<int>* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_SOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_INTEGER) {
			throw arcusTypeException();
		}

		return v.u.s_i;
	}
};
/*}}}*/
template <> struct arcusValueType<set<int> >/*{{{*/
{
	static set<int> decode(const arcusValue& v)
	{
		return *arcusValueType<set<int>* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<set<int64_t>* >/*{{{*/
{
	static set<int64_t>* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_SOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_LONG) {
			throw arcusTypeException();
		}

		return v.u.s_ll;
	}
};
/*}}}*/
template <> struct arcusValueType<set<int64_t> >/*{{{*/
{
	static set<int64_t> decode(const arcusValue& v)
	{
		return *arcusValueType<set<int64_t>* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<set<float>* >/*{{{*/
{
	static set<float>* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_SOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_FLOAT) {
			throw arcusTypeException();
		}

		return v.u.s_f;
	}
};
/*}}}*/
template <> struct arcusValueType<set<float> >/*{{{*/
{
	static set<float> decode(const arcusValue& v)
	{
		return *arcusValueType<set<float>* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<set<double>* >/*{{{*/
{
	static set<double>* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_SOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_DOUBLE) {
			throw arcusTypeException();
		}

		return v.u.s_d;
	}
};
/*}}}*/
template <> struct arcusValueType<set<double> >/*{{{*/
{
	static set<double> decode(const arcusValue& v)
	{
		return *arcusValueType<set<double>* >::decode(v);
	}
};
/*}}}*/
/*}}}*/

//
// map
////////////////////
/*{{{*/
// long key
template <> struct arcusValueType<map<uint64_t, arcusBopItem<string> >* >/*{{{*//*{{{*/
{
	static map<uint64_t, arcusBopItem<string> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_STRING) {
			throw arcusTypeException();
		}

		return v.u.m_l_s;
	}
};
/*}}}*/
template <> struct arcusValueType<map<uint64_t, arcusBopItem<string> > >/*{{{*/
{
	static map<uint64_t, arcusBopItem<string> >  decode(const arcusValue& v)
	{
		return *arcusValueType<map<uint64_t, arcusBopItem<string> >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<map<uint64_t, arcusBopItem<ptime> >* >/*{{{*/
{
	static map<uint64_t, arcusBopItem<ptime> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_DATE) {
			throw arcusTypeException();
		}

		return v.u.m_l_t;
	}
};
/*}}}*/
template <> struct arcusValueType<map<uint64_t, arcusBopItem<ptime> > >/*{{{*/
{
	static map<uint64_t, arcusBopItem<ptime> > decode(const arcusValue& v)
	{
		return *arcusValueType<map<uint64_t, arcusBopItem<ptime> >* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<map<uint64_t, arcusBopItem<vector<char> > >* >/*{{{*/
{
	static map<uint64_t, arcusBopItem<vector<char> > >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BYTEARRAY) {
			throw arcusTypeException();
		}

		return v.u.m_l_array;
	}
};
/*}}}*/
template <> struct arcusValueType<map<uint64_t, arcusBopItem<vector<char> > > >/*{{{*/
{
	static map<uint64_t, arcusBopItem<vector<char> > > decode(const arcusValue& v)
	{
		return *arcusValueType<map<uint64_t, arcusBopItem<vector<char> > >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<map<uint64_t, arcusBopItem<bool> >* >/*{{{*/
{
	static map<uint64_t, arcusBopItem<bool> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BOOLEAN) {
			throw arcusTypeException();
		}

		return v.u.m_l_b;
	}
};
/*}}}*/
template <> struct arcusValueType<map<uint64_t, arcusBopItem<bool> > >/*{{{*/
{
	static map<uint64_t, arcusBopItem<bool> > decode(const arcusValue& v)
	{
		return *arcusValueType<map<uint64_t, arcusBopItem<bool> >* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<map<uint64_t, arcusBopItem<char> >* >/*{{{*/
{
	static map<uint64_t, arcusBopItem<char> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BYTE) {
			throw arcusTypeException();
		}

		return v.u.m_l_c;
	}
};
/*}}}*/
template <> struct arcusValueType<map<uint64_t, arcusBopItem<char> > >/*{{{*/
{
	static map<uint64_t, arcusBopItem<char> > decode(const arcusValue& v)
	{
		return *arcusValueType<map<uint64_t, arcusBopItem<char> >* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<map<uint64_t, arcusBopItem<int> >* >/*{{{*/
{
	static map<uint64_t, arcusBopItem<int> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_INTEGER) {
			throw arcusTypeException();
		}

		return v.u.m_l_i;
	}
};
/*}}}*/
template <> struct arcusValueType<map<uint64_t, arcusBopItem<int> > >/*{{{*/
{
	static map<uint64_t, arcusBopItem<int> > decode(const arcusValue& v)
	{
		return *arcusValueType<map<uint64_t, arcusBopItem<int> >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<map<uint64_t, arcusBopItem<int64_t> >* >/*{{{*/
{
	static map<uint64_t, arcusBopItem<int64_t> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_LONG) {
			throw arcusTypeException();
		}

		return v.u.m_l_ll;
	}
};
/*}}}*/
template <> struct arcusValueType<map<uint64_t, arcusBopItem<int64_t> > >/*{{{*/
{
	static map<uint64_t, arcusBopItem<int64_t> > decode(const arcusValue& v)
	{
		return *arcusValueType<map<uint64_t, arcusBopItem<int64_t> >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<map<uint64_t, arcusBopItem<float> >* >/*{{{*/
{
	static map<uint64_t, arcusBopItem<float> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_FLOAT) {
			throw arcusTypeException();
		}

		return v.u.m_l_f;
	}
};
/*}}}*/
template <> struct arcusValueType<map<uint64_t, arcusBopItem<float> > >/*{{{*/
{
	static map<uint64_t, arcusBopItem<float> > decode(const arcusValue& v)
	{
		return *arcusValueType<map<uint64_t, arcusBopItem<float> >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<map<uint64_t, arcusBopItem<double> >* >/*{{{*/
{
	static map<uint64_t, arcusBopItem<double> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_DOUBLE) {
			throw arcusTypeException();
		}

		return v.u.m_l_d;
	}
};
/*}}}*/
template <> struct arcusValueType<map<uint64_t, arcusBopItem<double> > >/*{{{*/
{
	static map<uint64_t, arcusBopItem<double> > decode(const arcusValue& v)
	{
		return *arcusValueType<map<uint64_t, arcusBopItem<double> >* >::decode(v);
	}
};
/*}}}*/
/*}}}*/

// array key
template <> struct arcusValueType<map<vector<unsigned char>, arcusBopItem<string> >* >/*{{{*//*{{{*/
{
	static map<vector<unsigned char>, arcusBopItem<string> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_STRING) {
			throw arcusTypeException();
		}

		return v.u.m_v_s;
	}
};
/*}}}*/
template <> struct arcusValueType<map<vector<unsigned char>, arcusBopItem<string> > >/*{{{*/
{
	static map<vector<unsigned char>, arcusBopItem<string> > decode(const arcusValue& v)
	{
		return *arcusValueType<map<vector<unsigned char>, arcusBopItem<string> >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<map<vector<unsigned char>, arcusBopItem<ptime> >* >/*{{{*/
{
	static map<vector<unsigned char>, arcusBopItem<ptime> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_DATE) {
			throw arcusTypeException();
		}

		return v.u.m_v_t;
	}
};
/*}}}*/
template <> struct arcusValueType<map<vector<unsigned char>, arcusBopItem<ptime> > >/*{{{*/
{
	static map<vector<unsigned char>, arcusBopItem<ptime> > decode(const arcusValue& v)
	{
		return *arcusValueType<map<vector<unsigned char>, arcusBopItem<ptime> >* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<map<vector<unsigned char>, arcusBopItem<vector<char> > >* >/*{{{*/
{
	static map<vector<unsigned char>, arcusBopItem<vector<char> > >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BYTEARRAY) {
			throw arcusTypeException();
		}

		return v.u.m_v_array;
	}
};
/*}}}*/
template <> struct arcusValueType<map<vector<unsigned char>, arcusBopItem<vector<char> > > >/*{{{*/
{
	static map<vector<unsigned char>, arcusBopItem<vector<char> > > decode(const arcusValue& v)
	{
		return *arcusValueType<map<vector<unsigned char>, arcusBopItem<vector<char> > >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<map<vector<unsigned char>, arcusBopItem<bool> >* >/*{{{*/
{
	static map<vector<unsigned char>, arcusBopItem<bool> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BOOLEAN) {
			throw arcusTypeException();
		}

		return v.u.m_v_b;
	}
};
/*}}}*/
template <> struct arcusValueType<map<vector<unsigned char>, arcusBopItem<bool> > >/*{{{*/
{
	static map<vector<unsigned char>, arcusBopItem<bool> > decode(const arcusValue& v)
	{
		return *arcusValueType<map<vector<unsigned char>, arcusBopItem<bool> >* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<map<vector<unsigned char>, arcusBopItem<char> >* >/*{{{*/
{
	static map<vector<unsigned char>, arcusBopItem<char> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BYTE) {
			throw arcusTypeException();
		}

		return v.u.m_v_c;
	}
};
/*}}}*/
template <> struct arcusValueType<map<vector<unsigned char>, arcusBopItem<char> > >/*{{{*/
{
	static map<vector<unsigned char>, arcusBopItem<char> > decode(const arcusValue& v)
	{
		return *arcusValueType<map<vector<unsigned char>, arcusBopItem<char> >* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<map<vector<unsigned char>, arcusBopItem<int> >* >/*{{{*/
{
	static map<vector<unsigned char>, arcusBopItem<int> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_INTEGER) {
			throw arcusTypeException();
		}

		return v.u.m_v_i;
	}
};
/*}}}*/
template <> struct arcusValueType<map<vector<unsigned char>, arcusBopItem<int> > >/*{{{*/
{
	static map<vector<unsigned char>, arcusBopItem<int> > decode(const arcusValue& v)
	{
		return *arcusValueType<map<vector<unsigned char>, arcusBopItem<int> >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<map<vector<unsigned char>, arcusBopItem<int64_t> >* >/*{{{*/
{
	static map<vector<unsigned char>, arcusBopItem<int64_t> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_LONG) {
			throw arcusTypeException();
		}

		return v.u.m_v_ll;
	}
};
/*}}}*/
template <> struct arcusValueType<map<vector<unsigned char>, arcusBopItem<int64_t> > >/*{{{*/
{
	static map<vector<unsigned char>, arcusBopItem<int64_t> > decode(const arcusValue& v)
	{
		return *arcusValueType<map<vector<unsigned char>, arcusBopItem<int64_t> >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<map<vector<unsigned char>, arcusBopItem<float> >* >/*{{{*/
{
	static map<vector<unsigned char>, arcusBopItem<float> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_FLOAT) {
			throw arcusTypeException();
		}

		return v.u.m_v_f;
	}
};
/*}}}*/
template <> struct arcusValueType<map<vector<unsigned char>, arcusBopItem<float> > >/*{{{*/
{
	static map<vector<unsigned char>, arcusBopItem<float> > decode(const arcusValue& v)
	{
		return *arcusValueType<map<vector<unsigned char>, arcusBopItem<float> >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<map<vector<unsigned char>, arcusBopItem<double> >* >/*{{{*/
{
	static map<vector<unsigned char>, arcusBopItem<double> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_DOUBLE) {
			throw arcusTypeException();
		}

		return v.u.m_v_d;
	}
};
/*}}}*/
template <> struct arcusValueType<map<vector<unsigned char>, arcusBopItem<double> > >/*{{{*/
{
	static map<vector<unsigned char>, arcusBopItem<double> > decode(const arcusValue& v)
	{
		return *arcusValueType<map<vector<unsigned char>, arcusBopItem<double> >* >::decode(v);
	}
};
/*}}}*/
/*}}}*/
/*}}}*/

//
// smget map
////////////////////
/*{{{*/
// long key
template <> struct arcusValueType<multimap<uint64_t, arcusBopItem<string> >* >/*{{{*//*{{{*/
{
	static multimap<uint64_t, arcusBopItem<string> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_STRING) {
			throw arcusTypeException();
		}

		return v.u.mm_l_s;
	}
};
/*}}}*/
template <> struct arcusValueType<multimap<uint64_t, arcusBopItem<string> > >/*{{{*/
{
	static multimap<uint64_t, arcusBopItem<string> >  decode(const arcusValue& v)
	{
		return *arcusValueType<multimap<uint64_t, arcusBopItem<string> >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<multimap<uint64_t, arcusBopItem<ptime> >* >/*{{{*/
{
	static multimap<uint64_t, arcusBopItem<ptime> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_DATE) {
			throw arcusTypeException();
		}

		return v.u.mm_l_t;
	}
};
/*}}}*/
template <> struct arcusValueType<multimap<uint64_t, arcusBopItem<ptime> > >/*{{{*/
{
	static multimap<uint64_t, arcusBopItem<ptime> > decode(const arcusValue& v)
	{
		return *arcusValueType<multimap<uint64_t, arcusBopItem<ptime> >* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<multimap<uint64_t, arcusBopItem<vector<char> > >* >/*{{{*/
{
	static multimap<uint64_t, arcusBopItem<vector<char> > >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BYTEARRAY) {
			throw arcusTypeException();
		}

		return v.u.mm_l_array;
	}
};
/*}}}*/
template <> struct arcusValueType<multimap<uint64_t, arcusBopItem<vector<char> > > >/*{{{*/
{
	static multimap<uint64_t, arcusBopItem<vector<char> > > decode(const arcusValue& v)
	{
		return *arcusValueType<multimap<uint64_t, arcusBopItem<vector<char> > >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<multimap<uint64_t, arcusBopItem<bool> >* >/*{{{*/
{
	static multimap<uint64_t, arcusBopItem<bool> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BOOLEAN) {
			throw arcusTypeException();
		}

		return v.u.mm_l_b;
	}
};
/*}}}*/
template <> struct arcusValueType<multimap<uint64_t, arcusBopItem<bool> > >/*{{{*/
{
	static multimap<uint64_t, arcusBopItem<bool> > decode(const arcusValue& v)
	{
		return *arcusValueType<multimap<uint64_t, arcusBopItem<bool> >* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<multimap<uint64_t, arcusBopItem<char> >* >/*{{{*/
{
	static multimap<uint64_t, arcusBopItem<char> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BYTE) {
			throw arcusTypeException();
		}

		return v.u.mm_l_c;
	}
};
/*}}}*/
template <> struct arcusValueType<multimap<uint64_t, arcusBopItem<char> > >/*{{{*/
{
	static multimap<uint64_t, arcusBopItem<char> > decode(const arcusValue& v)
	{
		return *arcusValueType<multimap<uint64_t, arcusBopItem<char> >* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<multimap<uint64_t, arcusBopItem<int> >* >/*{{{*/
{
	static multimap<uint64_t, arcusBopItem<int> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_INTEGER) {
			throw arcusTypeException();
		}

		return v.u.mm_l_i;
		assert (false);
	}
};
/*}}}*/
template <> struct arcusValueType<multimap<uint64_t, arcusBopItem<int> > >/*{{{*/
{
	static multimap<uint64_t, arcusBopItem<int> > decode(const arcusValue& v)
	{
		return *arcusValueType<multimap<uint64_t, arcusBopItem<int> >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<multimap<uint64_t, arcusBopItem<int64_t> >* >/*{{{*/
{
	static multimap<uint64_t, arcusBopItem<int64_t> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_LONG) {
			throw arcusTypeException();
		}

		return v.u.mm_l_ll;
	}
};
/*}}}*/
template <> struct arcusValueType<multimap<uint64_t, arcusBopItem<int64_t> > >/*{{{*/
{
	static multimap<uint64_t, arcusBopItem<int64_t> > decode(const arcusValue& v)
	{
		return *arcusValueType<multimap<uint64_t, arcusBopItem<int64_t> >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<multimap<uint64_t, arcusBopItem<float> >* >/*{{{*/
{
	static multimap<uint64_t, arcusBopItem<float> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_FLOAT) {
			throw arcusTypeException();
		}

		return v.u.mm_l_f;
	}
};
/*}}}*/
template <> struct arcusValueType<multimap<uint64_t, arcusBopItem<float> > >/*{{{*/
{
	static multimap<uint64_t, arcusBopItem<float> > decode(const arcusValue& v)
	{
		return *arcusValueType<multimap<uint64_t, arcusBopItem<float> >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<multimap<uint64_t, arcusBopItem<double> >* >/*{{{*/
{
	static multimap<uint64_t, arcusBopItem<double> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_DOUBLE) {
			throw arcusTypeException();
		}

		return v.u.mm_l_d;
	}
};
/*}}}*/
template <> struct arcusValueType<multimap<uint64_t, arcusBopItem<double> > >/*{{{*/
{
	static multimap<uint64_t, arcusBopItem<double> > decode(const arcusValue& v)
	{
		return *arcusValueType<multimap<uint64_t, arcusBopItem<double> >* >::decode(v);
	}
};
/*}}}*/
/*}}}*/

// array key
template <> struct arcusValueType<multimap<vector<unsigned char>, arcusBopItem<string> >* >/*{{{*//*{{{*/
{
	static multimap<vector<unsigned char>, arcusBopItem<string> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_STRING) {
			throw arcusTypeException();
		}

		return v.u.mm_v_s;
	}
};
/*}}}*/
template <> struct arcusValueType<multimap<vector<unsigned char>, arcusBopItem<string> > >/*{{{*/
{
	static multimap<vector<unsigned char>, arcusBopItem<string> > decode(const arcusValue& v)
	{
		return *arcusValueType<multimap<vector<unsigned char>, arcusBopItem<string> >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<multimap<vector<unsigned char>, arcusBopItem<ptime> >* >/*{{{*/
{
	static multimap<vector<unsigned char>, arcusBopItem<ptime> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_DATE) {
			throw arcusTypeException();
		}

		return v.u.mm_v_t;
	}
};
/*}}}*/
template <> struct arcusValueType<multimap<vector<unsigned char>, arcusBopItem<ptime> > >/*{{{*/
{
	static multimap<vector<unsigned char>, arcusBopItem<ptime> > decode(const arcusValue& v)
	{
		return *arcusValueType<multimap<vector<unsigned char>, arcusBopItem<ptime> >* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<multimap<vector<unsigned char>, arcusBopItem<vector<char> > >* >/*{{{*/
{
	static multimap<vector<unsigned char>, arcusBopItem<vector<char> > >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BYTEARRAY) {
			throw arcusTypeException();
		}

		return v.u.mm_v_array;
	}
};
/*}}}*/
template <> struct arcusValueType<multimap<vector<unsigned char>, arcusBopItem<vector<char> > > >/*{{{*/
{
	static multimap<vector<unsigned char>, arcusBopItem<vector<char> > > decode(const arcusValue& v)
	{
		return *arcusValueType<multimap<vector<unsigned char>, arcusBopItem<vector<char> > >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<multimap<vector<unsigned char>, arcusBopItem<bool> >* >/*{{{*/
{
	static multimap<vector<unsigned char>, arcusBopItem<bool> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BOOLEAN) {
			throw arcusTypeException();
		}

		return v.u.mm_v_b;
	}
};
/*}}}*/
template <> struct arcusValueType<multimap<vector<unsigned char>, arcusBopItem<bool> > >/*{{{*/
{
	static multimap<vector<unsigned char>, arcusBopItem<bool> > decode(const arcusValue& v)
	{
		return *arcusValueType<multimap<vector<unsigned char>, arcusBopItem<bool> >* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<multimap<vector<unsigned char>, arcusBopItem<char> >* >/*{{{*/
{
	static multimap<vector<unsigned char>, arcusBopItem<char> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BYTE) {
			throw arcusTypeException();
		}

		return v.u.mm_v_c;
	}
};
/*}}}*/
template <> struct arcusValueType<multimap<vector<unsigned char>, arcusBopItem<char> > >/*{{{*/
{
	static multimap<vector<unsigned char>, arcusBopItem<char> > decode(const arcusValue& v)
	{
		return *arcusValueType<multimap<vector<unsigned char>, arcusBopItem<char> >* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<multimap<vector<unsigned char>, arcusBopItem<int> >* >/*{{{*/
{
	static multimap<vector<unsigned char>, arcusBopItem<int> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_INTEGER) {
			throw arcusTypeException();
		}

		return v.u.mm_v_i;
	}
};
/*}}}*/
template <> struct arcusValueType<multimap<vector<unsigned char>, arcusBopItem<int> > >/*{{{*/
{
	static multimap<vector<unsigned char>, arcusBopItem<int> > decode(const arcusValue& v)
	{
		return *arcusValueType<multimap<vector<unsigned char>, arcusBopItem<int> >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<multimap<vector<unsigned char>, arcusBopItem<int64_t> >* >/*{{{*/
{
	static multimap<vector<unsigned char>, arcusBopItem<int64_t> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_LONG) {
			throw arcusTypeException();
		}

		return v.u.mm_v_ll;
	}
};
/*}}}*/
template <> struct arcusValueType<multimap<vector<unsigned char>, arcusBopItem<int64_t> > >/*{{{*/
{
	static multimap<vector<unsigned char>, arcusBopItem<int64_t> > decode(const arcusValue& v)
	{
		return *arcusValueType<multimap<vector<unsigned char>, arcusBopItem<int64_t> >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<multimap<vector<unsigned char>, arcusBopItem<float> >* >/*{{{*/
{
	static multimap<vector<unsigned char>, arcusBopItem<float> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_FLOAT) {
			throw arcusTypeException();
		}

		return v.u.mm_v_f;
	}
};
/*}}}*/
template <> struct arcusValueType<multimap<vector<unsigned char>, arcusBopItem<float> > >/*{{{*/
{
	static multimap<vector<unsigned char>, arcusBopItem<float> > decode(const arcusValue& v)
	{
		return *arcusValueType<multimap<vector<unsigned char>, arcusBopItem<float> >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<multimap<vector<unsigned char>, arcusBopItem<double> >* >/*{{{*/
{
	static multimap<vector<unsigned char>, arcusBopItem<double> >* decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_DOUBLE) {
			throw arcusTypeException();
		}

		return v.u.mm_v_d;
	}
};
/*}}}*/
template <> struct arcusValueType<multimap<vector<unsigned char>, arcusBopItem<double> > >/*{{{*/
{
	static multimap<vector<unsigned char>, arcusBopItem<double> > decode(const arcusValue& v)
	{
		return *arcusValueType<multimap<vector<unsigned char>, arcusBopItem<double> >* >::decode(v);
	}
};
/*}}}*/
/*}}}*/
/*}}}*/
//
// mget map
//////////////////////
/*{{{*/
// long key
template <> struct arcusValueType<map<string, map<uint64_t, arcusBopItem<string> >* > >/*{{{*//*{{{*/
{
	static map<string, map<uint64_t, arcusBopItem<string> > > * decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_STRING) {
			throw arcusTypeException();
		}

		return v.u.mget_l_s;
	}
};
/*}}}*/
template <> struct arcusValueType<map<string, map<uint64_t, arcusBopItem<string> > > >/*{{{*/
{
	static map<string, map<uint64_t, arcusBopItem<string> > > decode(const arcusValue& v)
	{
		return *arcusValueType<map<string, map<uint64_t, arcusBopItem<string> > >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<map<string, map<uint64_t, arcusBopItem<ptime> >* > >/*{{{*/
{
	static map<string, map<uint64_t, arcusBopItem<ptime> > > * decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_DATE) {
			throw arcusTypeException();
		}

		return v.u.mget_l_t;
	}
};
/*}}}*/
template <> struct arcusValueType<map<string, map<uint64_t, arcusBopItem<ptime> > > >/*{{{*/
{
	static map<string, map<uint64_t, arcusBopItem<ptime> > > decode(const arcusValue& v)
	{
		return *arcusValueType<map<string, map<uint64_t, arcusBopItem<ptime> > >* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<map<string, map<uint64_t, arcusBopItem<vector<char> > > >* >/*{{{*/
{
	static map<string, map<uint64_t, arcusBopItem<vector<char> > > > * decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BYTEARRAY) {
			throw arcusTypeException();
		}

		return v.u.mget_l_array;
	}
};
/*}}}*/
template <> struct arcusValueType<map<string, map<uint64_t, arcusBopItem<vector<char> > > > >/*{{{*/
{
	static map<string, map<uint64_t, arcusBopItem<vector<char> > > > decode(const arcusValue& v)
	{
		return *arcusValueType<map<string, map<uint64_t, arcusBopItem<vector<char> > > >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<map<string, map<uint64_t, arcusBopItem<bool> > >* >/*{{{*/
{
	static map<string, map<uint64_t, arcusBopItem<bool> > > * decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BOOLEAN) {
			throw arcusTypeException();
		}

		return v.u.mget_l_b;
	}
};
/*}}}*/
template <> struct arcusValueType<map<string, map<uint64_t, arcusBopItem<bool> > > >/*{{{*/
{
	static map<string, map<uint64_t, arcusBopItem<bool> > > decode(const arcusValue& v)
	{
		return *arcusValueType<map<string, map<uint64_t, arcusBopItem<bool> > >* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<map<string, map<uint64_t, arcusBopItem<char> > >* >/*{{{*/
{
	static map<string, map<uint64_t, arcusBopItem<char> > > * decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BYTE) {
			throw arcusTypeException();
		}

		return v.u.mget_l_c;
	}
};
/*}}}*/
template <> struct arcusValueType<map<string, map<uint64_t, arcusBopItem<char> > > >/*{{{*/
{
	static map<string, map<uint64_t, arcusBopItem<char> > > decode(const arcusValue& v)
	{
		return *arcusValueType<map<string, map<uint64_t, arcusBopItem<char> > >* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<map<string, map<uint64_t, arcusBopItem<int> > >* >/*{{{*/
{
	static map<string, map<uint64_t, arcusBopItem<int> > > * decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_INTEGER) {
			throw arcusTypeException();
		}

		return v.u.mget_l_i;
	}
};
/*}}}*/
template <> struct arcusValueType<map<string, map<uint64_t, arcusBopItem<int> > > >/*{{{*/
{
	static map<string, map<uint64_t, arcusBopItem<int> > > decode(const arcusValue& v)
	{
		return *arcusValueType<map<string, map<uint64_t, arcusBopItem<int> > >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<map<string, map<uint64_t, arcusBopItem<int64_t> > >* >/*{{{*/
{
	static map<string, map<uint64_t, arcusBopItem<int64_t> > > * decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_LONG) {
			throw arcusTypeException();
		}

		return v.u.mget_l_ll;
	}
};
/*}}}*/
template <> struct arcusValueType<map<string, map<uint64_t, arcusBopItem<int64_t> > > >/*{{{*/
{
	static map<string, map<uint64_t, arcusBopItem<int64_t> > > decode(const arcusValue& v)
	{
		return *arcusValueType<map<string, map<uint64_t, arcusBopItem<int64_t> > >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<map<string, map<uint64_t, arcusBopItem<float> > >* >/*{{{*/
{
	static map<string, map<uint64_t, arcusBopItem<float> > > * decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_FLOAT) {
			throw arcusTypeException();
		}

		return v.u.mget_l_f;
	}
};
/*}}}*/
template <> struct arcusValueType<map<string, map<uint64_t, arcusBopItem<float> > > >/*{{{*/
{
	static map<string, map<uint64_t, arcusBopItem<float> > > decode(const arcusValue& v)
	{
		return *arcusValueType<map<string, map<uint64_t, arcusBopItem<float> > >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<map<string, map<uint64_t, arcusBopItem<double> > >* >/*{{{*/
{
	static map<string, map<uint64_t, arcusBopItem<double> > > * decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_DOUBLE) {
			throw arcusTypeException();
		}

		return v.u.mget_l_d;
	}
};
/*}}}*/
template <> struct arcusValueType<map<string, map<uint64_t, arcusBopItem<double> > > >/*{{{*/
{
	static map<string, map<uint64_t, arcusBopItem<double> > > decode(const arcusValue& v)
	{
		return *arcusValueType<map<string, map<uint64_t, arcusBopItem<double> > >* >::decode(v);
	}
};
/*}}}*/
/*}}}*/

// array key
template <> struct arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<string> > >* >/*{{{*//*{{{*/
{
	static map<string, map<vector<unsigned char>, arcusBopItem<string> > > * decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_STRING) {
			throw arcusTypeException();
		}

		return v.u.mget_v_s;
	}
};
/*}}}*/
template <> struct arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<string> > > >/*{{{*/
{
	static map<string, map<vector<unsigned char>, arcusBopItem<string> > > decode(const arcusValue& v)
	{
		return *arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<string> > >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<ptime> > >* >/*{{{*/
{
	static map<string, map<vector<unsigned char>, arcusBopItem<ptime> > > * decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_DATE) {
			throw arcusTypeException();
		}

		return v.u.mget_v_t;
	}
};
/*}}}*/
template <> struct arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<ptime> > > >/*{{{*/
{
	static map<string, map<vector<unsigned char>, arcusBopItem<ptime> > > decode(const arcusValue& v)
	{
		return *arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<ptime> > >* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<vector<char> > > >* >/*{{{*/
{
	static map<string, map<vector<unsigned char>, arcusBopItem<vector<char> > > > * decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BYTEARRAY) {
			throw arcusTypeException();
		}

		return v.u.mget_v_array;
	}
};
/*}}}*/
template <> struct arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<vector<char> > > > >/*{{{*/
{
	static map<string, map<vector<unsigned char>, arcusBopItem<vector<char> > > > decode(const arcusValue& v)
	{
		return *arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<vector<char> > > >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<bool> > >* >/*{{{*/
{
	static map<string, map<vector<unsigned char>, arcusBopItem<bool> > > * decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BOOLEAN) {
			throw arcusTypeException();
		}

		return v.u.mget_v_b;
	}
};
/*}}}*/
template <> struct arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<bool> > > >/*{{{*/
{
	static map<string, map<vector<unsigned char>, arcusBopItem<bool> > > decode(const arcusValue& v)
	{
		return *arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<bool> > >* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<char> > >* >/*{{{*/
{
	static map<string, map<vector<unsigned char>, arcusBopItem<char> > > * decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_BYTE) {
			throw arcusTypeException();
		}

		return v.u.mget_v_c;
	}
};
/*}}}*/
template <> struct arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<char> > > >/*{{{*/
{
	static map<string, map<vector<unsigned char>, arcusBopItem<char> > > decode(const arcusValue& v)
	{
		return *arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<char> > >* >::decode(v);
	}
};

/*}}}*/

template <> struct arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<int> > >* >/*{{{*/
{
	static map<string, map<vector<unsigned char>, arcusBopItem<int> > > * decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_INTEGER) {
			throw arcusTypeException();
		}

		return v.u.mget_v_i;
	}
};
/*}}}*/
template <> struct arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<int> > > >/*{{{*/
{
	static map<string, map<vector<unsigned char>, arcusBopItem<int> > > decode(const arcusValue& v)
	{
		return *arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<int> > >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<int64_t> > >* >/*{{{*/
{
	static map<string, map<vector<unsigned char>, arcusBopItem<int64_t> > > * decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_LONG) {
			throw arcusTypeException();
		}

		return v.u.mget_v_ll;
	}
};
/*}}}*/
template <> struct arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<int64_t> > > >/*{{{*/
{
	static map<string, map<vector<unsigned char>, arcusBopItem<int64_t> > > decode(const arcusValue& v)
	{
		return *arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<int64_t> > >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<float> > >* >/*{{{*/
{
	static map<string, map<vector<unsigned char>, arcusBopItem<float> > > * decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_FLOAT) {
			throw arcusTypeException();
		}

		return v.u.mget_v_f;
	}
};
/*}}}*/
template <> struct arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<float> > > >/*{{{*/
{
	static map<string, map<vector<unsigned char>, arcusBopItem<float> > > decode(const arcusValue& v)
	{
		return *arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<float> > >* >::decode(v);
	}
};
/*}}}*/

template <> struct arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<double> > >* >/*{{{*/
{
	static map<string, map<vector<unsigned char>, arcusBopItem<double> > > * decode(const arcusValue& v)
	{
		if (v.cflags != COLL_BOP) {
			throw arcusTypeException();
		}

		if ((v.flags & FLAG_MASK) != FLAG_DOUBLE) {
			throw arcusTypeException();
		}

		return v.u.mget_v_d;
	}
};
/*}}}*/
template <> struct arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<double> > > >/*{{{*/
{
	static map<string, map<vector<unsigned char>, arcusBopItem<double> > > decode(const arcusValue& v)
	{
		return *arcusValueType<map<string, map<vector<unsigned char>, arcusBopItem<double> > >* >::decode(v);
	}
};
/*}}}*/
/*}}}*/
/*}}}*/




/*}}}*/

class arcusOperation/*{{{*/
{
public:
	arcusOperation(arcusNode* n = NULL, const char* r = NULL, int len = 0)/*{{{*/
	{
		node = n;
		memcpy(request, r, len);
		rlen = len;
		invalid = false;
		ready = false;

		pthread_mutex_init(&mutex, NULL);
		pthread_cond_init(&cond, NULL);
	}
/*}}}*/

	virtual ~arcusOperation() /*{{{*/
	{
		pthread_mutex_destroy(&mutex);
		pthread_cond_destroy(&cond);

		if (ready) {
			switch (value.cflags)
			{
			case COLL_LOP:
				switch (value.flags & FLAG_MASK)
				{
				case FLAG_INTEGER:
					delete value.u.l_i;
					break;

				case FLAG_STRING:
					delete value.u.l_s;
					break;
				}

				break;

			case COLL_SOP:
				break;

			case COLL_BOP:
				break;

			case COLL_NONE:
				break;

			default:
				break;
			}

		}
	}
/*}}}*/

	bool has_result()
	{
		return ready;
	}

	void set_result(const arcusValue& result)/*{{{*/
	{
		pthread_mutex_lock(&mutex);
		value = result;
		ready = true;
		pthread_mutex_unlock(&mutex);

		pthread_cond_signal(&cond);
	}
/*}}}*/

	void set_invalid()
	{
		invalid = true;
	}

	template <typename T>
	T get_result()/*{{{*/
	{
		pthread_mutex_lock(&mutex);
		if (ready == false) {
			pthread_cond_wait(&cond, &mutex);
		}
		pthread_mutex_unlock(&mutex);

		check_exception();
		return arcusValueType<T>::decode(value);
	}
/*}}}*/

	template <typename K, typename V>
	map<K, arcusBopItem<V> > get_result_map()/*{{{*/
	{
		pthread_mutex_lock(&mutex);
		if (ready == false) {
			pthread_cond_wait(&cond, &mutex);
		}
		pthread_mutex_unlock(&mutex);

		check_exception();
		return arcusValueType<map<K, arcusBopItem<V> > >::decode(value);
	}
/*}}}*/

	template <typename K, typename V>
	map<K, arcusBopItem<V> >* get_result_map_ptr()/*{{{*/
	{
		pthread_mutex_lock(&mutex);
		if (ready == false) {
			pthread_cond_wait(&cond, &mutex);
		}
		pthread_mutex_unlock(&mutex);

		check_exception();
		return arcusValueType<map<K, arcusBopItem<V> >* >::decode(value);
	}
/*}}}*/

	template <typename K, typename V>
	multimap<K, arcusBopItem<V> >* get_result_multimap_ptr()/*{{{*/
	{
		pthread_mutex_lock(&mutex);
		if (ready == false) {
			pthread_cond_wait(&cond, &mutex);
		}
		pthread_mutex_unlock(&mutex);

		check_exception();
		return arcusValueType<multimap<K, arcusBopItem<V> >* >::decode(value);
	}
/*}}}*/

	template <typename K, typename V>
	map<string, map<K, arcusBopItem<V> > >* get_mget_result_ptr()/*{{{*/
	{
		pthread_mutex_lock(&mutex);
		if (ready == false) {
			pthread_cond_wait(&cond, &mutex);
		}
		pthread_mutex_unlock(&mutex);

		check_exception();
		return arcusValueType<map<string, map<K, arcusBopItem<V> > >* >::decode(value);
	}
/*}}}*/

private:
	void check_exception()/*{{{*/
	{
		switch (value.exception)
		{
		case ARCUS_EXCEPTION:							throw arcusException(); break;
		case ARCUS_PROTOCOL_EXCEPTION:					throw arcusProtocolException(); break;
		case ARCUS_TYPE_EXCEPTION:						throw arcusTypeException(); break;
		case ARCUS_NODE_EXCEPTION:						throw arcusNodeException(); break;
		case ARCUS_NODE_SOCKET_EXCEPTION:				throw arcusNodeSocketException(); break;
		case ARCUS_NODE_CONNECTION_EXCEPTION:			throw arcusNodeConnectionException(); break;
		case ARCUS_COLLECTION_EXCEPTION:				throw arcusCollectionException(); break;
		case ARCUS_COLLECTION_TYPE_EXCEPTION:			throw arcusCollectionTypeException(); break;
		case ARCUS_COLLECTION_EXISTS_EXCEPTION:			throw arcusCollectionExistsException(); break;
		case ARCUS_COLLECTION_NOT_FOUND_EXCEPTION:		throw arcusCollectionNotFoundException(); break;
		case ARCUS_COLLECTION_INDEX_EXCEPTION:			throw arcusCollectionIndexException(); break;
		case ARCUS_COLLECTION_OVERFLOW_EXCEPTION:		throw arcusCollectionOverflowException(); break;
		case ARCUS_COLLECTION_UNREADABLE_EXCEPTION:		throw arcusCollectionUnreadableException(); break;
		case ARCUS_COLLECTION_HEX_FORMAT_EXCEPTION:		throw arcusCollectionHexFormatException(); break;
		case ARCUS_COLLECTION_FILTER_INVALID_EXCEPTION:	throw arcusCollectionFilterInvalidException(); break;
		}
	}
/*}}}*/

public:
	arcusNode* node;
	char request[MAX_ELEMENT_SIZE + MAX_CMD_LEN];
	int rlen;

	bool ready;
	arcusValue value;
	bool invalid;
	pthread_mutex_t	mutex;
	pthread_cond_t 	cond;
};
/*}}}*/

class arcusOperationList/*{{{*/
{
public:
	arcusOperationList(const char* r = NULL, int len = 0)/*{{{*/
	{
		memcpy(request, r, len);
		rlen = len;
	}
/*}}}*/

	virtual ~arcusOperationList() /*{{{*/
	{
	}
/*}}}*/

	void add_op(const arcusFuture& ft)
	{
		ops.push_back(ft);
	}

	vector<string> get_missed_keys()
	{
		return missed_keys;
	}

	bool has_result()/*{{{*/
	{
		for (int i=0; i<ops.size(); i++) {
			if (ops[i]->has_result() == false) {
				return false;
			}
		}

		return true;
	}
/*}}}*/

	void set_invalid()/*{{{*/
	{
		for (int i=0; i<ops.size(); i++) {
			ops[i]->set_invalid();
		}
	}
/*}}}*/

	template <typename K, typename V>
	multimap<K, arcusBopItem<V> > get_smget_result()/*{{{*/
	{
		multimap<K, arcusBopItem<V> > ret;

		for (int i=0; i<ops.size(); i++) {
			multimap<K, arcusBopItem<V> >* tmp = ops[i]->get_result_multimap_ptr<K, V>();
			ret.insert(tmp->begin(), tmp->end());

			copy(ops[i]->value.missed_keys.begin(), ops[i]->value.missed_keys.end(), back_inserter(missed_keys));
		}
		
		sort(missed_keys.begin(), missed_keys.end());
		return ret;
	}
/*}}}*/

	template <typename K, typename V>
	map<string, map<K, arcusBopItem<V> > >get_mget_result()/*{{{*/
	{
		map<string, map<K, arcusBopItem<V> > > ret;

		for (int i=0; i<ops.size(); i++) {
			map<string, map<K, arcusBopItem<V> > >* tmp = ops[i]->get_mget_result_ptr<K, V>();
			ret.insert(tmp->begin(), tmp->end());

			copy(ops[i]->value.missed_keys.begin(), ops[i]->value.missed_keys.end(), back_inserter(missed_keys));
		}
		
		sort(missed_keys.begin(), missed_keys.end());
		return ret;
	}
/*}}}*/


public:
	char request[MAX_CMD_LEN];
	int rlen;

	vector<arcusFuture> ops;
	vector<string> missed_keys;
};
/*}}}*/


class arcusNode/*{{{*/
{
public:
	arcusNode(const string& addr, const string& name)
	{
		this->addr = addr;
		this->name = name;
	}

	virtual ~arcusNode() {}

	virtual arcusFuture set(const string& key, const arcusValue& v, int exptime=0) = 0;
	virtual arcusFuture get(const string& key) = 0;
	virtual arcusFuture gets(const string& key) = 0;
	virtual arcusFuture incr(const string& key, int v= 1) = 0;
	virtual arcusFuture decr(const string& key, int v= 1) = 0;
	virtual arcusFuture del(const string& key) = 0;
	virtual arcusFuture add(const string& key, const arcusValue& v, int exptime=0) = 0;
	virtual arcusFuture append(const string& key, const string& v, int exptime=0) = 0;
	virtual arcusFuture prepend(const string& key, const string& v, int exptime=0) = 0;
	virtual arcusFuture replace(const string& key, const arcusValue& v, int exptime=0) = 0;
	virtual arcusFuture cas(const string& key, const arcusValue& v, int cas_id, int exptime=0) = 0;
	virtual arcusFuture lop_create(const string& key, ARCUS_TYPE type, int exptime=0, const arcusAttribute& attrs = default_attrs) = 0;
	virtual arcusFuture lop_insert(const string& key, int index, const arcusValue& value, const arcusAttribute& attrs = default_attrs) = 0;
	virtual arcusFuture lop_get(const string& key, int start = 0, int end = -1, const arcusAttribute& attrs = default_attrs) = 0;
	virtual arcusFuture lop_delete(const string& key, int start = 0, int end = -1, const arcusAttribute& attrs = default_attrs) = 0;

	virtual arcusFuture sop_create(const string& key, ARCUS_TYPE type, int exptime=0, const arcusAttribute& attrs = default_attrs) = 0;
	virtual arcusFuture sop_insert(const string& key, const arcusValue& value, const arcusAttribute& attrs = default_attrs) = 0;
	virtual arcusFuture sop_get(const string& key, int count = 0, const arcusAttribute& attrs = default_attrs) = 0;
	virtual arcusFuture sop_exist(const string& key, const arcusValue& value, const arcusAttribute& attrs = default_attrs) = 0;
	virtual arcusFuture sop_delete(const string& key, const arcusValue& value, const arcusAttribute& attrs = default_attrs) = 0;

	virtual arcusFuture bop_create(const string& key, ARCUS_TYPE type, int exptime=0, const arcusAttribute& attrs = default_attrs) = 0;
	virtual arcusFuture bop_insert(const string& key, const arcusBKey& bkey, const arcusValue& value, const arcusEflag& eflag = eflag_none, const arcusAttribute& attrs = default_attrs) = 0;
	virtual arcusFuture bop_upsert(const string& key, const arcusBKey& bkey, const arcusValue& value, const arcusEflag& eflag = eflag_none, const arcusAttribute& attrs = default_attrs) = 0;
	virtual arcusFuture bop_update(const string& key, const arcusBKey& bkey, const arcusValue& value, const arcusEflag& eflag = eflag_none, const arcusAttribute& attrs = default_attrs) = 0;
	virtual arcusFuture bop_get(const string& key, const arcusBKey& start, const arcusBKey& end, const arcusEflag& filter = default_filter, const arcusAttribute& attrs = default_attrs) = 0;
	virtual arcusFuture bop_delete(const string& key, const arcusBKey& start, const arcusBKey& end, const arcusEflag& filter = default_filter, const arcusAttribute& attrs = default_attrs) = 0;
	virtual arcusFuture bop_count(const string& key, const arcusBKey& start, const arcusBKey& end, const arcusEflag& filter = default_filter) = 0;

	virtual arcusFuture bop_mget(const vector<string>& key_list, const arcusBKey& start, const arcusBKey& end, const arcusEflag& filter = default_filter, int offset = 0, int count = 50) = 0;

	virtual arcusFuture bop_smget(const vector<string>& key_list, const arcusBKey& start, const arcusBKey& end, const arcusEflag& filter = default_filter, int offset = 0, int count = 50) = 0;

	virtual int disconnect() = 0;

public:
	string addr;
	string name;
	bool in_use;
};
/*}}}*/

// dummy node for util
class arcusDummyNode : public arcusNode/*{{{*/
{
public:
	arcusDummyNode(const string& addr, const string& name) : arcusNode(addr, name) {}

	virtual ~arcusDummyNode() {}

	// dummy functions
	/*{{{*/
		virtual arcusFuture set(const string& key, const arcusValue& v, int exptime=0) {}
		virtual arcusFuture get(const string& key) {}
		virtual arcusFuture gets(const string& key) {}
		virtual arcusFuture incr(const string& key, int v= 1) {}
		virtual arcusFuture decr(const string& key, int v= 1) {}
		virtual arcusFuture del(const string& key) {}
		virtual arcusFuture add(const string& key, const arcusValue& v, int exptime=0) {}
		virtual arcusFuture append(const string& key, const string& v, int exptime=0) {}
		virtual arcusFuture prepend(const string& key, const string& v, int exptime=0) {}
		virtual arcusFuture replace(const string& key, const arcusValue& v, int exptime=0) {}
		virtual arcusFuture cas(const string& key, const arcusValue& v, int cas_id, int exptime=0) {}
		virtual arcusFuture lop_create(const string& key, ARCUS_TYPE type, int exptime=0, const arcusAttribute& attrs = default_attrs) {}
		virtual arcusFuture lop_insert(const string& key, int index, const arcusValue& value, const arcusAttribute& attrs = default_attrs) {}
		virtual arcusFuture lop_get(const string& key, int start = 0, int end = -1, const arcusAttribute& attrs = default_attrs) {}
		virtual arcusFuture lop_delete(const string& key, int start = 0, int end = -1, const arcusAttribute& attrs = default_attrs) {}

		virtual arcusFuture sop_create(const string& key, ARCUS_TYPE type, int exptime=0, const arcusAttribute& attrs = default_attrs) {}
		virtual arcusFuture sop_insert(const string& key, const arcusValue& value, const arcusAttribute& attrs = default_attrs) {}
		virtual arcusFuture sop_get(const string& key, int count = 0, const arcusAttribute& attrs = default_attrs) {}
		virtual arcusFuture sop_exist(const string& key, const arcusValue& value, const arcusAttribute& attrs = default_attrs) {}
		virtual arcusFuture sop_delete(const string& key, const arcusValue& value, const arcusAttribute& attrs = default_attrs) {}

		virtual arcusFuture bop_create(const string& key, ARCUS_TYPE type, int exptime=0, const arcusAttribute& attrs = default_attrs) {}
		virtual arcusFuture bop_insert(const string& key, const arcusBKey& bkey, const arcusValue& value, const arcusEflag& eflag = eflag_none, const arcusAttribute& attrs = default_attrs) {}
		virtual arcusFuture bop_upsert(const string& key, const arcusBKey& bkey, const arcusValue& value, const arcusEflag& eflag = eflag_none, const arcusAttribute& attrs = default_attrs) {}
		virtual arcusFuture bop_update(const string& key, const arcusBKey& bkey, const arcusValue& value, const arcusEflag& eflag = eflag_none, const arcusAttribute& attrs = default_attrs) {}
		virtual arcusFuture bop_get(const string& key, const arcusBKey& start, const arcusBKey& end, const arcusEflag& filter = default_filter, const arcusAttribute& attrs = default_attrs) {}
		virtual arcusFuture bop_delete(const string& key, const arcusBKey& start, const arcusBKey& end, const arcusEflag& filter = default_filter, const arcusAttribute& attrs = default_attrs) {}
		virtual arcusFuture bop_count(const string& key, const arcusBKey& start, const arcusBKey& end, const arcusEflag& filter = default_filter) {}

		virtual arcusFuture bop_mget(const vector<string>& key_list, const arcusBKey& start, const arcusBKey& end, const arcusEflag& filter = default_filter, int offset = 0, int count = 50) {}

		virtual arcusFuture bop_smget(const vector<string>& key_list, const arcusBKey& start, const arcusBKey& end, const arcusEflag& filter = default_filter, int offset = 0, int count = 50) {}

		virtual int disconnect() {}
/*}}}*/
};
/*}}}*/

class arcusNodeAllocator/*{{{*/
{
public:
	virtual ~arcusNodeAllocator() {}
	virtual arcusNode* alloc(const string& addr, const string& name) = 0;
};
/*}}}*/


class arcusLocator;
typedef void (*zookeeper_watcher)(void *zk);

struct Zookeeper/*{{{*/
{
	Zookeeper(void* p)
	{
		// init default
		maxbytes = 0;
		last_rc = !ZOK;
		port = 0;
		param = p;

		myid.client_id = 0;
	}

	int connect(const string& addr, const string& svc_code, zookeeper_watcher zwatcher, arcusLocator* loc, int tmout=15000)
	{
		ensemble_list = addr;
		service_code = svc_code;
		session_timeout = tmout;
		watcher = zwatcher;
		locator = loc;

		handle = zookeeper_init(ensemble_list.c_str(), arcus_watcher, session_timeout, &myid, this, 0);
		if (handle) return 0;
		
		return -1;
	}

	int disconnect()
	{
		zookeeper_close(handle);
		return 0;
	}

	vector<string> get_children(const string& path)
	{
		vector<string> str;
		struct String_vector strings;
		if (zoo_get_children(handle, path.c_str(), 1, &strings) == ZOK) {
			for (int i=0; i<strings.count; i++) {
				str.push_back(strings.data[i]);
			}
		}

		return str;
	}

	static void arcus_watcher(zhandle_t *zh, int type, int state, const char *path, void *zk_p)
	{
		int rc;
		Zookeeper* zk = (Zookeeper*)zk_p;

		if (type == ZOO_CHILD_EVENT) {
			zk->watcher(zk->locator);
		}
		
		if (type != ZOO_SESSION_EVENT) {
			return;
		}

		if (state == ZOO_CONNECTED_STATE) {
			arcus_log("SESSION_STATE=CONNECTED, to %s", zk->ensemble_list.c_str());

			const clientid_t *id= zoo_client_id(zh);

			if (zk->myid.client_id == 0 or zk->myid.client_id != id->client_id) {
				arcus_log("Previous sessionid : 0x%llx", (int64_t) zk->myid.client_id);
				zk->myid= *id;
				arcus_log("Current sessionid	: 0x%llx", (int64_t) zk->myid.client_id);
			}

			zk->watcher(zk->param);
		}
		else if (state == ZOO_CONNECTING_STATE or state == ZOO_ASSOCIATING_STATE) {
			arcus_log("SESSION_STATE=CONNECTING, to %s", zk->ensemble_list.c_str());
		}
		else if (state == ZOO_AUTH_FAILED_STATE) {
			arcus_log("SESSION_STATE=AUTH_FAILED, shutting down the application");
		}
		else if (state == ZOO_EXPIRED_SESSION_STATE) {
			arcus_log("SESSION_STATE=EXPIRED_SESSION, create a new client after closing expired one");

			/* Respawn the expired zookeeper client. */
			zk->disconnect();
			zk->connect(zk->ensemble_list, zk->service_code, zk->watcher, zk->locator, zk->session_timeout);
		}
	}


	zhandle_t *handle;
	clientid_t myid;
	string ensemble_list;
	string service_code;
	uint32_t session_timeout;

	char path[256];
	uint32_t port;
	size_t maxbytes;
	int last_rc;
	struct String_vector last_strings;

	zookeeper_watcher watcher;
	arcusLocator* locator;
	void* param;
};
/*}}}*/


class arcusLocator/*{{{*/
{
public:
	arcusLocator(arcusNodeAllocator* allocator)
	{
		hash_method = new arcusKetemaHash();
		node_allocator = allocator;
		zoo = new Zookeeper(this);

		pthread_mutex_init(&mutex, NULL);
	}

	virtual ~arcusLocator() {}

	static void arcus_watcher(void *vp)/*{{{*/
	{

		arcusLocator *locator = (arcusLocator*)vp;
		Zookeeper *zk = locator->zoo;

		pthread_mutex_lock(&locator->mutex);
		arcus_log("Watcher: arcusLocator.arcus_watcher called\n");

		vector<string> ret = zk->get_children("/arcus/cache_list/" + zk->service_code);
		
		for (int i=0; i<ret.size(); i++) {
			arcus_log("Watcher list: %s\n", ret[i].c_str());
		}

		locator->hash_nodes(ret);

		pthread_mutex_unlock(&locator->mutex);
	}
/*}}}*/

	int connect(const string& addr, const string& code)
	{
		int ret = zoo->connect(addr, code, arcus_watcher, this);
		arcus_watcher(this);
		return ret;
	}

	int disconnect()
	{
		return zoo->disconnect();
	}

	void hash_nodes(vector<string>& children)/*{{{*/
	{
		arcus_log("## rehashing node\n");

		// use tmp while rehashing
		hash_node_map_tmp.clear();
		copy(hash_node_map_curr.begin(), hash_node_map_curr.end(), inserter(hash_node_map_tmp, hash_node_map_tmp.begin()));
		hash_node_map = &hash_node_map_tmp;

		// clear first
		hash_node_map_curr.clear();

		map<string, arcusNode*>::iterator it;
		for (it = addr_node_map.begin(); it != addr_node_map.end(); ++it) {
			it->second->in_use = false;
		}

		for (int i=0; i<children.size(); i++) {
			string child = children[i];
			int idx = child.find_first_of("-");
			
			string addr = child.substr(0, idx);
			string name = child.substr(idx);
			printf("%s-%s-%s\n", child.c_str(), addr.c_str(), name.c_str());

			arcusNode* node;
			if (addr_node_map.find(addr) != addr_node_map.end()) {
				addr_node_map[addr]->in_use = true;
				node = addr_node_map[addr];
			}
			else {
				node = node_allocator->alloc(addr, name);
				addr_node_map[addr] = node;
				addr_node_map[addr]->in_use = true;
			}

			vector<unsigned int> hash_list = hash_method->hash(addr);
			for (int i=0; i<hash_list.size(); i++) {
				hash_node_map_curr[hash_list[i]] = node;
			}
		}
		
		// disconnect dead node
		for (it = addr_node_map.begin(); it != addr_node_map.end(); ++it) {
			if (it->second->in_use == false) {
				arcusNode* node = it->second;
				node->disconnect();
				delete node;  

				addr_node_map.erase(it->first);
			}
		}

		// all done. use curr
		hash_node_map = &hash_node_map_curr;
	}
/*}}}*/

	size_t num_node()
	{
		return hash_node_map->size();
	}

	arcusNode* get_node(const string& key)/*{{{*/
	{
		unsigned int hash = hash_key(key);

		pthread_mutex_lock(&mutex);
		map<unsigned int, arcusNode*>::iterator it = hash_node_map->find(hash);
		if (it != hash_node_map->end()) {
			pthread_mutex_unlock(&mutex);
			return it->second;
		}

		it = hash_node_map->upper_bound(hash);
		if (it == hash_node_map->end()) {
			// if end return first (cause circle)
			assert (hash_node_map->size() > 0);
			pthread_mutex_unlock(&mutex);
			return hash_node_map->begin()->second;
		}

		pthread_mutex_unlock(&mutex);
		return it->second;
	}
/*}}}*/

private:
	unsigned int hash_key(const string& key)
	{
		unsigned char r[16];

		MHASH td = mhash_init(MHASH_MD5);
		mhash(td, (unsigned char*)key.c_str(), key.length());
		mhash_deinit(td, r);

		return r[3] << 24 | r[2] << 16 | r[1] << 8 | r[0];
	}

	Zookeeper *zoo;
	map<string, arcusNode*> addr_node_map;
	map<unsigned int, arcusNode*> *hash_node_map;

	map<unsigned int, arcusNode*> hash_node_map_curr;
	map<unsigned int, arcusNode*> hash_node_map_tmp;

	arcusNodeAllocator* node_allocator;
	arcusHash* hash_method;
	pthread_mutex_t mutex;
};
/*}}}*/


class arcus/*{{{*/
{
public:
	arcus(arcusLocator* l)
	{
		locator = l;
	}

	int connect(const string& addr, const string& code)
	{
		return locator->connect(addr, code);
	}

	int disconnect()
	{
		return locator->disconnect();
	}

	template <typename T>
	arcusFuture set(const string& key, const T& value, int exptime=0)
	{
		arcusNode* node = locator->get_node(key);
		return node->set(key, value, exptime);
	}

	arcusFuture get(const string& key)
	{
		arcusNode* node = locator->get_node(key);
		return node->get(key);
	}

	arcusFuture gets(const string& key)
	{
		arcusNode* node = locator->get_node(key);
		return node->gets(key);
	}

	arcusFuture incr(const string& key, int value = 1)
	{
		arcusNode* node = locator->get_node(key);
		return node->incr(key, value);
	}

	arcusFuture decr(const string& key, int value = 1)
	{
		arcusNode* node = locator->get_node(key);
		return node->decr(key, value);
	}

	arcusFuture del(const string& key)
	{
		arcusNode* node = locator->get_node(key);
		return node->del(key);
	}

	template <typename T>
	arcusFuture add(const string& key, const T& value, int exptime=0)
	{
		arcusNode* node = locator->get_node(key);
		return node->add(key, value, exptime);
	}

	arcusFuture append(const string& key, const string& value, int exptime=0)
	{
		arcusNode* node = locator->get_node(key);
		return node->add(key, value, exptime);
	}

	arcusFuture prepend(const string& key, const string& value, int exptime=0)
	{
		arcusNode* node = locator->get_node(key);
		return node->prepend(key, value, exptime);
	}

	template <typename T>
	arcusFuture replace(const string& key, const T& value, int exptime=0)
	{
		arcusNode* node = locator->get_node(key);
		return node->replace(key, value, exptime);
	}

	template <typename T>
	arcusFuture cas(const string& key, const T& value, int cas_id, int exptime=0)
	{
		arcusNode* node = locator->get_node(key);
		return node->cas(key, value, cas_id, exptime);
	}

	arcusFuture lop_create(const string& key, ARCUS_TYPE type, int exptime=0, const arcusAttribute& attrs = default_attrs)
	{
		arcusNode* node = locator->get_node(key);
		return node->lop_create(key, type, exptime, attrs);
	}

	template <typename T>
	arcusFuture lop_insert(const string& key, int index, const T& value, const arcusAttribute& attrs = default_attrs)
	{
		arcusNode* node = locator->get_node(key);
		return node->lop_insert(key, index, value, attrs);
	}

	arcusFuture lop_get(const string& key, int start = 0, int end = -1, const arcusAttribute& attrs = default_attrs)
	{
		arcusNode* node = locator->get_node(key);
		return node->lop_get(key, start, end, attrs);
	}

	arcusFuture lop_delete(const string& key, int start = 0, int end = -1, const arcusAttribute& attrs = default_attrs)
	{
		arcusNode* node = locator->get_node(key);
		return node->lop_delete(key, start, end, attrs);
	}

	arcusFuture sop_create(const string& key, ARCUS_TYPE type, int exptime=0, const arcusAttribute& attrs = default_attrs)
	{
		arcusNode* node = locator->get_node(key);
		return node->sop_create(key, type, exptime, attrs);
	}

	template <typename T>
	arcusFuture sop_insert(const string& key, const T& value, const arcusAttribute& attrs = default_attrs)
	{
		arcusNode* node = locator->get_node(key);
		return node->sop_insert(key, value, attrs);
	}

	arcusFuture sop_get(const string& key, int count = 0, const arcusAttribute& attrs = default_attrs)
	{
		arcusNode* node = locator->get_node(key);
		return node->sop_get(key, count, attrs);
	}

	template <typename T>
	arcusFuture sop_exist(const string& key, const T& value, const arcusAttribute& attrs = default_attrs)
	{
		arcusNode* node = locator->get_node(key);
		return node->sop_exist(key, value, attrs);
	}

	template <typename T>
	arcusFuture sop_delete(const string& key, const T& value, const arcusAttribute& attrs = default_attrs)
	{
		arcusNode* node = locator->get_node(key);
		return node->sop_delete(key, value, attrs);
	}

	arcusFuture bop_create(const string& key, ARCUS_TYPE type, int exptime=0, const arcusAttribute& attrs = default_attrs)
	{
		arcusNode* node = locator->get_node(key);
		return node->bop_create(key, type, exptime, attrs);
	}

	template <typename K, typename T>
	arcusFuture bop_insert(const string& key, const K& bkey, const T& value, const arcusEflag& eflag = eflag_none, const arcusAttribute& attrs = default_attrs)
	{
		arcusNode* node = locator->get_node(key);
		return node->bop_insert(key, bkey, value, eflag, attrs);
	}

	template <typename K, typename T>
	arcusFuture bop_upsert(const string& key, const K& bkey, const T& value, const arcusEflag& eflag = eflag_none, const arcusAttribute& attrs = default_attrs)
	{
		arcusNode* node = locator->get_node(key);
		return node->bop_upsert(key, value, eflag, attrs);
	}

	template <typename K, typename T>
	arcusFuture bop_update(const string& key, const K& bkey, const T& value, const arcusEflag& eflag = eflag_none, const arcusAttribute& attrs = default_attrs)
	{
		arcusNode* node = locator->get_node(key);
		return node->bop_upsert(key, value, eflag, attrs);
	}

	template <typename K>
	arcusFuture bop_get(const string& key, const K& start, const K& end, const arcusEflag& filter = default_filter, const arcusAttribute& attrs = default_attrs)
	{
		arcusNode* node = locator->get_node(key);
		return node->bop_get(key, start, end, filter, attrs);
	}

	template <typename K>
	arcusFuture bop_delete(const string& key, const K& start, const K& end, const arcusEflag& filter = default_filter, const arcusAttribute& attrs = default_attrs)
	{
		arcusNode* node = locator->get_node(key);
		return node->bop_delete(key, start, end, filter, attrs);
	}

	template <typename K>
	arcusFuture bop_count(const string& key, const K& start, const K& end, const arcusEflag& filter = default_filter)
	{
		arcusNode* node = locator->get_node(key);
		return node->bop_count(key, start, end, filter);
	}

	template <typename K>
	arcusFutureList bop_mget(const vector<string>& key_list, const K& start, const K& end, const arcusEflag& filter = default_filter, int offset = 0, int count = 50)
	{
		arcusFutureList ft(new arcusOperationList("bop_mget", 8));
		std::set<arcusNode*> nodes;

		for (int i=0; i<key_list.size(); i++) {
			arcusNode* node = locator->get_node(key_list[i]);
			nodes.insert(node);
		}

		for (std::set<arcusNode*>::iterator it = nodes.begin(); it != nodes.end(); ++it) {
			ft->add_op((*it)->bop_mget(key_list, start, end, filter, offset, count));
		}

		return ft;
	}

	template <typename K>
	arcusFutureList bop_smget(const vector<string>& key_list, const K& start, const K& end, const arcusEflag& filter = default_filter, int offset = 0, int count = 50)
	{
		arcusFutureList ft(new arcusOperationList("bop_smget", 9));
		std::set<arcusNode*> nodes;

		for (int i=0; i<key_list.size(); i++) {
			arcusNode* node = locator->get_node(key_list[i]);
			nodes.insert(node);
		}

		for (std::set<arcusNode*>::iterator it = nodes.begin(); it != nodes.end(); ++it) {
			ft->add_op((*it)->bop_smget(key_list, start, end, filter, offset, count));
		}

		return ft;
	}
private:
	arcusLocator* locator;
};
/*}}}*/

#endif




