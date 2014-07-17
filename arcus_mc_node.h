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

#ifndef __DEF_ARCUS_MC_NODE__
#define __DEF_ARCUS_MC_NODE__



#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/epoll.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <sys/ioctl.h>


// boost
#include <boost/tuple/tuple.hpp>
#include <boost/lexical_cast.hpp>

using namespace boost;

#include "arcus.h"



class arcusMCNode;
class arcusMCWorker;

template <typename T>
void process_request_helper(arcusMCNode* node, const char* req, int rlen);

struct arcusSystem/*{{{*/
{
	static bool shutdown(bool f = false)
	{
		static bool flag = false;

		if (f == true) {
			flag = true;
		}

		return flag;
	}
};
/*}}}*/

class threadClass/*{{{*/
{
public:
	virtual void run() = 0;

	static void* entry_point(void* vp)
	{
		threadClass *tc = (threadClass*)vp;

		tc->run();
	}

	void start()
	{
		tid = pthread_create(&thread, NULL, entry_point, this);
	}

	void join()
	{
		pthread_join(thread, NULL);
	}

private:
	pthread_t thread;
	int tid;
};
/*}}}*/

class arcusMCWorker : public threadClass/*{{{*/
{
public:
	static arcusMCWorker* handle()
	{
		static arcusMCWorker* w = NULL;
		
		if (w == NULL) {
			w = new arcusMCWorker();
		}

		return w;
	}

	virtual void run()
	{
		arcus_log("worker: start\n");
		while(true) {
			arcusFuture op = q.pop();
			if (arcusSystem::shutdown() == true) {
				return;
			}

			arcus_log("worker: get operation (%p)%s\n", op.get(), op->request);
			arcusMCNode* node = (arcusMCNode*)op->node;
			process_request_helper<arcusMCNode>(node, op->request, op->rlen);
		}
	}

public:
	arcusMCWorker() { }
	async_queue<arcusFuture> q;
};
/*}}}*/

struct arcusMCConnection/*{{{*/
{
	arcusMCConnection() { }

	int connect(const string& addr_port)
	{
		int idx = addr_port.find_first_of(":");
		string addr = addr_port.substr(0, idx);
		int port;
		try {
			port = lexical_cast<int>(addr_port.substr(idx+1));
		}
		catch (bad_lexical_cast) {
			arcus_log("invalid port: %s\n", addr_port.substr(idx+1).c_str());
			return -1;
		}

		struct sockaddr_in server_addr;
		memset(&server_addr, 0, sizeof(server_addr));
		server_addr.sin_family = AF_INET;
		server_addr.sin_port = htons(port);
		server_addr.sin_addr.s_addr = inet_addr(addr.c_str());
		
		sock = socket(PF_INET, SOCK_STREAM, 0);
		int ret = ::connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr));
		// TODO : error

		arcus_log("Connection: [%s:%d] sock: %d connect -> %d\n", addr.c_str(), port, sock, ret);
	}

	int disconnect()
	{
		close(sock);
		sock = 0;
	}

	bool disconnected()
	{
		return sock == 0;
	}

	int send_request(const char* request, int rlen)
	{
		int ret = write(sock, request, rlen);
		arcus_log("Connection: send(%d): %s -> %d\n", sock, request, ret);
		return ret;
	}

	int find(const char* target, int tlen)
	{
		int slen = buff_len;
		int i, j;

		for (i=0; i<slen; i++) {
			for (j=0; j<tlen; j++) {
				if (buffer[i+j] != target[j])
					break;

				if (i+j > slen)
					break;

			}

			if (j == tlen) {
				return i;
			}
		}

		return -1;
	}

	bool hasline()
	{
		int idx = find("\r\n", 2);
		return idx >= 0;
	}

	string readline()
	{
		int idx = -1;
		while (true) {
			idx = find("\r\n", 2);
			if (idx >= 0) {
				break;
			}

			char tmp[4096];
			int ret = read(sock, tmp, sizeof(tmp));
			// TODO ret 0, -1

			memcpy(buffer + buff_len, tmp, ret);
			buff_len += ret;
		}

		char line[sizeof(buffer)];
		memcpy(line, buffer, idx);
		line[idx] = 0;

		idx += 2; // \r\n
		memmove(buffer, buffer + idx, buff_len - idx);
		buff_len -= idx;

		arcus_log("Connection: readline: '%s'\n", line);
		return line;
	}

	int readdata(char* buff, int len)
	{
		int ret;
		int idx = -1;
		while (true) {
			idx = find("\r\n", 2);
			if (idx >= 0) {
				break;
			}

			char tmp[4096];
			int ret = read(sock, tmp, sizeof(tmp));
			// TODO ret 0, -1

			memcpy(buffer + buff_len, tmp, ret);
			buff_len += ret;
		}

		char line[sizeof(buffer)];
		memcpy(line, buffer, idx);
		line[idx] = 0;
		ret = idx;

		idx += 2; // \r\n
		memmove(buffer, buffer + idx, buff_len - idx);
		buff_len -= idx;

		memcpy (buff, line, ret);
		buff[ret] = 0;

		arcus_log("Connection: readdata: '%s'\n", buff);
		return ret;
	}

	int recv(char* data, int rlen)
	{
		while (buff_len < rlen) {
			char tmp[4096];
			int ret = read(sock, tmp, sizeof(tmp));
			// TODO ret 0, -1

			memcpy(buffer + buff_len, tmp, ret);
			buff_len += ret;
		}

		memcpy (data, buffer, rlen);
		arcus_log("Connection: recv(%d): %s\n", rlen, data);

		memmove(buffer, buffer + rlen, buff_len - rlen);
		buff_len -= rlen;

		return rlen;
	}

	int sock;
	char buffer[1024*128];
	int buff_len;
};
/*}}}*/


typedef arcusValue (arcusMCNode::*member_fn)();

class arcusMCOperation : public arcusOperation/*{{{*/
{
public:
	arcusMCOperation(arcusNode* n, const char* cmd, int cmdlen, member_fn cb) : arcusOperation(n, cmd, cmdlen)
	{
		callback = cb;
	}
	

public:
	member_fn callback;
};
/*}}}*/

class arcusMCNode : public arcusNode
{
public:
	arcusMCNode(const string& addr, const string&name, arcusTranscoder* tc) : arcusNode(addr, name)/*{{{*/
	{
		transcoder = tc;
		pthread_mutex_init(&mutex, NULL);

		handle.connect(addr);
	}
/*}}}*/

	virtual ~arcusMCNode()/*{{{*/
	{

	}
/*}}}*/

//
// commands
//////////////////////
	virtual arcusFuture set(const string& key, const arcusValue& value, int exptime=0)/*{{{*/
	{
		return _set("set", key, value, exptime);
	}
/*}}}*/

	virtual arcusFuture get(const string& key)/*{{{*/
	{
		return _get("get", key);
	}
/*}}}*/

	virtual arcusFuture gets(const string& key)/*{{{*/
	{
		return _get("gets", key);
	}
/*}}}*/

	virtual arcusFuture incr(const string& key, int v= 1)/*{{{*/
	{
		return _incr_decr("incr", key, v);
	}
/*}}}*/

	virtual arcusFuture decr(const string& key, int v= 1)/*{{{*/
	{
		return _incr_decr("decr", key, v);
	}
/*}}}*/

	virtual arcusFuture del(const string& key)/*{{{*/
	{
		char buff[MAX_CMD_LEN];
		int bufflen;

		bufflen = snprintf(buff, sizeof(buff), "delete %s\r\n", key.c_str());

		return add_op("delete", buff, bufflen, &arcusMCNode::cb_delete);
	}
/*}}}*/

	virtual arcusFuture add(const string& key, const arcusValue& v, int exptime=0)/*{{{*/
	{
		return _set("add", key, v, exptime);
	}
/*}}}*/

	virtual arcusFuture append(const string& key, const string& v, int exptime=0)/*{{{*/
	{
		return _set("append", key, v, exptime);
	}
/*}}}*/

	virtual arcusFuture prepend(const string& key, const string& v, int exptime=0)/*{{{*/
	{
		return _set("prepend", key, v, exptime);
	}
/*}}}*/

	virtual arcusFuture replace(const string& key, const arcusValue& v, int exptime=0)/*{{{*/
	{
		return _set("replace", key, v, exptime);
	}
/*}}}*/

	virtual arcusFuture cas(const string& key, const arcusValue& v, int cas_id, int exptime=0)/*{{{*/
	{
		return _set("cas", key, v, exptime, cas_id);
	}
/*}}}*/

	virtual arcusFuture lop_create(const string& key, ARCUS_TYPE type,/*{{{*/
							 	   int exptime = 0, const arcusAttribute& attrs = default_attrs)
	{
		return _coll_create("lop create", key, type, exptime, attrs);
	}
/*}}}*/

	virtual arcusFuture lop_insert(const string& key, int index, const arcusValue& value, /*{{{*/
								   const arcusAttribute& attrs = default_attrs)
	{
		char buff[MAX_ELEMENT_SIZE + MAX_CMD_LEN];
		int bufflen;

		arcusData ret = transcoder->encode(value);

		if (attrs.create) {
			bufflen = snprintf(buff, sizeof(buff), "lop insert %s %d %d create %d %d %d%s%s%s%s\r\n",
							   key.c_str(), index, ret.value.size(), 
							   attrs.flags, attrs.exptime, attrs.max_count,
							   attrs.get_ovfl_action(), attrs.readable?"":" unreadable",
							   attrs.noreply?" noreply":"", attrs.pipe?" pipe":"");
		}
		else {
			bufflen = snprintf(buff, sizeof(buff), "lop insert %s %d %d%s%s\r\n",
							   key.c_str(), index, ret.value.size(), 
							   attrs.noreply?" noreply":"", attrs.pipe?" pipe":"");
		}

		memcpy(buff + bufflen, &ret.value[0], ret.value.size());
		bufflen += ret.value.size();
		memcpy(buff + bufflen, "\r\n", 2);
		bufflen += 2;

		return add_op("lop insert", buff, bufflen, &arcusMCNode::cb_coll_set);
	}
/*}}}*/

	virtual arcusFuture lop_get(const string& key, int start = 0, int end = -1, const arcusAttribute& attrs = default_attrs)/*{{{*/
	{
		char buff[MAX_CMD_LEN];
		int bufflen;

		bufflen = snprintf(buff, sizeof(buff), "lop get %s %d..%d%s%s\r\n",
						   key.c_str(), start, end, attrs.del?" delete":"", attrs.drop?" drop":"");

		return add_op("lop get", buff, bufflen, &arcusMCNode::cb_lop_get);
	}
/*}}}*/

	virtual arcusFuture lop_delete(const string& key, int start = 0, int end = -1, const arcusAttribute& attrs = default_attrs)/*{{{*/
	{
		char buff[MAX_CMD_LEN];
		int bufflen;

		bufflen = snprintf(buff, sizeof(buff), "lop delete %s %d..%d%s%s%s\r\n",
						   key.c_str(), start, end, attrs.drop?" drop":"",
						   attrs.noreply?" noreply":"", attrs.pipe?" pipe":"");

		return add_op("lop delete", buff, bufflen, &arcusMCNode::cb_delete);
	}
/*}}}*/

	virtual arcusFuture sop_create(const string& key, ARCUS_TYPE type,/*{{{*/
							 	   int exptime = 0, const arcusAttribute& attrs = default_attrs)
	{
		return _coll_create("sop create", key, type, exptime, attrs);
	}
/*}}}*/

	virtual arcusFuture sop_insert(const string& key, const arcusValue& value, /*{{{*/
								   const arcusAttribute& attrs = default_attrs)
	{
		char buff[MAX_ELEMENT_SIZE + MAX_CMD_LEN];
		int bufflen;

		arcusData ret = transcoder->encode(value);

		if (attrs.create) {
			bufflen = snprintf(buff, sizeof(buff), "sop insert %s %d create %d %d %d%s%s%s%s\r\n",
							   key.c_str(), ret.value.size(), 
							   attrs.flags, attrs.exptime, attrs.max_count,
							   attrs.get_ovfl_action(), attrs.readable?"":" unreadable",
							   attrs.noreply?" noreply":"", attrs.pipe?" pipe":"");
		}
		else {
			bufflen = snprintf(buff, sizeof(buff), "sop insert %s %d%s%s\r\n",
							   key.c_str(), ret.value.size(), 
							   attrs.noreply?" noreply":"", attrs.pipe?" pipe":"");
		}

		memcpy(buff + bufflen, &ret.value[0], ret.value.size());
		bufflen += ret.value.size();
		memcpy(buff + bufflen, "\r\n", 2);
		bufflen += 2;

		return add_op("sop insert", buff, bufflen, &arcusMCNode::cb_coll_set);
	}
/*}}}*/

	virtual arcusFuture sop_get(const string& key, int count = 0, const arcusAttribute& attrs = default_attrs)/*{{{*/
	{
		char buff[MAX_CMD_LEN];
		int bufflen;

		bufflen = snprintf(buff, sizeof(buff), "sop get %s %d%s%s\r\n",
						   key.c_str(), count, attrs.del?" delete":"", attrs.drop?" drop":"");

		return add_op("sop get", buff, bufflen, &arcusMCNode::cb_sop_get);
	}
/*}}}*/

	virtual arcusFuture sop_exist(const string& key, const arcusValue& value, const arcusAttribute& attrs = default_attrs)/*{{{*/
	{
		char buff[MAX_ELEMENT_SIZE + MAX_CMD_LEN];
		int bufflen;

		arcusData ret = transcoder->encode(value);

		bufflen = snprintf(buff, sizeof(buff), "sop exist %s %d%s\r\n",
						   key.c_str(), ret.value.size(), attrs.pipe?" pipe":"");

		memcpy(buff + bufflen, &ret.value[0], ret.value.size());
		bufflen += ret.value.size();
		memcpy(buff + bufflen, "\r\n", 2);
		bufflen += 2;

		return add_op("sop exist", buff, bufflen, &arcusMCNode::cb_sop_exist);
	}
/*}}}*/

	virtual arcusFuture sop_delete(const string& key, const arcusValue& value, const arcusAttribute& attrs = default_attrs)/*{{{*/
	{
		char buff[MAX_ELEMENT_SIZE + MAX_CMD_LEN];
		int bufflen;

		arcusData ret = transcoder->encode(value);

		bufflen = snprintf(buff, sizeof(buff), "sop delete %s %d%s%s%s\r\n", key.c_str(), ret.value.size(),
						   attrs.drop?" drop":"", attrs.del?" delete":"", attrs.pipe?" pipe":"");

		memcpy(buff + bufflen, &ret.value[0], ret.value.size());
		bufflen += ret.value.size();
		memcpy(buff + bufflen, "\r\n", 2);
		bufflen += 2;

		return add_op("sop delete", buff, bufflen, &arcusMCNode::cb_delete);
	}
/*}}}*/

	virtual arcusFuture bop_create(const string& key, ARCUS_TYPE type, int exptime=0, const arcusAttribute& attrs = default_attrs)/*{{{*/
	{
		return _coll_create("bop create", key, type, exptime, attrs);
	}
/*}}}*/

	virtual arcusFuture bop_insert(const string& key, const arcusBKey& bkey, const arcusValue& value, const arcusEflag& eflag = eflag_none, const arcusAttribute& attrs = default_attrs)/*{{{*/
	{
		return _bop_set("bop insert", key, bkey, value, eflag, attrs);
	}/*}}}*/

	virtual arcusFuture bop_upsert(const string& key, const arcusBKey& bkey, const arcusValue& value, const arcusEflag& eflag = eflag_none, const arcusAttribute& attrs = default_attrs)/*{{{*/
	{
		return _bop_set("bop upsert", key, bkey, value, eflag, attrs);
	}
/*}}}*/

	virtual arcusFuture bop_update(const string& key, const arcusBKey& bkey, const arcusValue& value, const arcusEflag& eflag = eflag_none, const arcusAttribute& attrs = default_attrs)/*{{{*/
	{
		return _bop_set("bop update", key, bkey, value, eflag, attrs);
	}
/*}}}*/

	virtual arcusFuture bop_get(const string& key, const arcusBKey& start, const arcusBKey& end, const arcusEflag& filter = default_filter, const arcusAttribute& attrs = default_attrs)/*{{{*/
	{
		char buff[MAX_CMD_LEN];
		int bufflen;

		bufflen = snprintf(buff, sizeof(buff), "bop get %s %s..%s %s%s%s\r\n",
						   key.c_str(), start.to_str().c_str(), end.to_str().c_str(), 
						   filter.to_str().c_str(), attrs.del?" delete":"", attrs.drop?" drop":"");

		return add_op("bop get", buff, bufflen, &arcusMCNode::cb_bop_get);
	}
/*}}}*/

	virtual arcusFuture bop_delete(const string& key, const arcusBKey& start, const arcusBKey& end, const arcusEflag& filter = default_filter, const arcusAttribute& attrs = default_attrs)/*{{{*/
	{
		char buff[MAX_CMD_LEN];
		int bufflen;

		bufflen = snprintf(buff, sizeof(buff), "bop delete %s %d..%d%s%s%s\r\n",
						   key.c_str(), start.to_str().c_str(), end.to_str().c_str(), attrs.drop?" drop":"",
						   attrs.noreply?" noreply":"", attrs.pipe?" pipe":"");

		return add_op("bop delete", buff, bufflen, &arcusMCNode::cb_delete);
	}
/*}}}*/

	virtual arcusFuture bop_count(const string& key, const arcusBKey& start, const arcusBKey& end, const arcusEflag& filter = default_filter)/*{{{*/
	{
		char buff[MAX_CMD_LEN];
		int bufflen;

		bufflen = snprintf(buff, sizeof(buff), "bop count %s %s..%s %s\r\n",
						   key.c_str(), start.to_str().c_str(), end.to_str().c_str(), filter.to_str().c_str());

		return add_op("bop get", buff, bufflen, &arcusMCNode::cb_bop_get);
	}
/*}}}*/

	virtual arcusFuture bop_mget(const vector<string>& key_list, const arcusBKey& start, const arcusBKey& end, const arcusEflag& filter = default_filter, int offset = 0, int count = 50)/*{{{*/
	{
		char buff[MAX_CMD_LEN];
		int bufflen;

		string comma_sep_keys;

		for (int i=0; i<key_list.size(); i++) {
			if (comma_sep_keys != "") {
				comma_sep_keys += ",";
			}

			comma_sep_keys += key_list[i];
		}

		bufflen = snprintf(buff, sizeof(buff), "bop mget %d %d %s..%s %s %s %s\r\n",
						   comma_sep_keys.length(), key_list.size(),
						   start.to_str().c_str(), end.to_str().c_str(), filter.to_str().c_str(), 
						   offset > 0 ? lexical_cast<string>(offset).c_str() : "",
						   lexical_cast<string>(count).c_str());

		memcpy(buff+ bufflen, comma_sep_keys.c_str(), comma_sep_keys.length());
		bufflen += comma_sep_keys.length();
		memcpy(buff+ bufflen, "\r\n", 2);
		bufflen += 2;

		return add_op("bop mget", buff, bufflen, &arcusMCNode::cb_bop_mget);
	}
/*}}}*/

	virtual arcusFuture bop_smget(const vector<string>& key_list, const arcusBKey& start, const arcusBKey& end, const arcusEflag& filter = default_filter, int offset = 0, int count = 50)/*{{{*/
	{
		char buff[MAX_CMD_LEN];
		int bufflen;

		string comma_sep_keys;

		for (int i=0; i<key_list.size(); i++) {
			if (comma_sep_keys != "") {
				comma_sep_keys += ",";
			}

			comma_sep_keys += key_list[i];
		}

		bufflen = snprintf(buff, sizeof(buff), "bop smget %d %d %s..%s %s %s %s\r\n",
						   comma_sep_keys.length(), key_list.size(),
						   start.to_str().c_str(), end.to_str().c_str(), filter.to_str().c_str(), 
						   offset > 0 ? lexical_cast<string>(offset).c_str() : "",
						   lexical_cast<string>(count).c_str());

		memcpy(buff+ bufflen, comma_sep_keys.c_str(), comma_sep_keys.length());
		bufflen += comma_sep_keys.length();
		memcpy(buff+ bufflen, "\r\n", 2);
		bufflen += 2;

		return add_op("bop smget", buff, bufflen, &arcusMCNode::cb_bop_smget);
	}
/*}}}*/

//
// operations
/////////////////////////
	virtual int disconnect()/*{{{*/
	{
		handle.disconnect();

		list<arcusFuture>::iterator it = ops.begin();
		for (; it != ops.end(); ++it) {
			(*it)->set_invalid();
		}

		ops.clear();
		return 0;
	}
/*}}}*/

	virtual int disconnect_all()/*{{{*/
	{
		arcusSystem::shutdown(true);
		disconnect();

		arcusMCWorker::handle()->q.push(arcusFuture(new arcusOperation()));
		return 0;
	}
/*}}}*/

	void process_request(const char* req, int rlen)/*{{{*/
	{
		handle.send_request(req, rlen);
	}
/*}}}*/

	arcusFuture add_op(const string& cmd, const char* full_cmd, int cmd_len, member_fn callback, bool noreply = false)/*{{{*/
	{
		arcusFuture op(new arcusMCOperation(this, full_cmd, cmd_len, callback));

		if (noreply) {
			arcusMCWorker::handle()->q.push(op);
			op->set_result(true);
		}
		else {
			pthread_mutex_lock(&mutex);
			arcusMCWorker::handle()->q.push(op);
			ops.push_back(op);
			pthread_mutex_unlock(&mutex);
		}

		return op;
	}
/*}}}*/

	void do_op()/*{{{*/
	{
		pthread_mutex_lock(&mutex);
		if (ops.empty()) {
			assert (false);
		}

		arcusFuture ft = *ops.begin();
		ops.pop_front();
		pthread_mutex_unlock(&mutex);

		
		arcusMCOperation* op = (arcusMCOperation*)ft.get();
		arcusValue ret = (this->*(op->callback))();
		op->set_result(ret);

		while (handle.hasline()) { // remaining jobs
			pthread_mutex_lock(&mutex);
			ft = *ops.begin();
			ops.pop_front();
			pthread_mutex_unlock(&mutex);

			arcusMCOperation* op = (arcusMCOperation*)ft.get();
			arcusValue ret = (this->*(op->callback))();
			op->set_result(ret);
		}
	}
/*}}}*/

	int get_fileno()/*{{{*/
	{
		return handle.sock;
	}
/*}}}*/

private:
//
// command subfucntions
//////////////////////////
	arcusFuture _set(const string& cmd, const string& key, const arcusValue& value, int exptime, int cas_id=-1)/*{{{*/
	{
		char _buff[4096];
		char *buff = _buff;
		int bufflen = sizeof(_buff);

		arcusData ret = transcoder->encode(value);

		if (ret.value.size() + 256 > bufflen) {
			bufflen = ret.value.size() + MAX_CMD_LEN;
			buff= (char*)malloc(bufflen);
		}

		if (cas_id != -1) {
			bufflen = snprintf(buff, bufflen, "%s %s %d %d %d\r\n",
						cmd.c_str(), key.c_str(), ret.flags, exptime, ret.value.size());
		}
		else {
			bufflen = snprintf(buff, bufflen, "%s %s %d %d %d %d\r\n",
						cmd.c_str(), key.c_str(), ret.flags, exptime, ret.value.size(), cas_id);
		}

		memcpy(buff+ bufflen, &ret.value[0], ret.value.size());
		bufflen += ret.value.size();
		memcpy(buff+ bufflen, "\r\n", 2);
		bufflen += 2;

		arcusFuture ft = add_op(cmd, buff, bufflen, &arcusMCNode::cb_set);

		if (buff!= _buff) {
			free(buff);
		}

		return ft;
	}
/*}}}*/

	arcusFuture _get(const string& cmd, const string& key)/*{{{*/
	{
		char buff[MAX_CMD_LEN];
		int bufflen;

		bufflen = snprintf(buff, sizeof(buff), "%s %s\r\n", cmd.c_str(), key.c_str());

		arcusFuture ft;
		if (cmd == "gets") {
			ft = add_op(cmd, buff, bufflen, &arcusMCNode::cb_gets);
		}
		else {
			ft = add_op(cmd, buff, bufflen, &arcusMCNode::cb_get);
		}

		return ft;
	}
/*}}}*/

	arcusFuture _incr_decr(const string& cmd, const string& key, int value)/*{{{*/
	{
		char buff[MAX_CMD_LEN];
		int bufflen;

		bufflen = snprintf(buff, sizeof(buff), "%s %s %d\r\n", cmd.c_str(), key.c_str(), value);

		arcusFuture ft;
		ft = add_op(cmd, buff, bufflen, &arcusMCNode::cb_incr_decr);

		return ft;
	}
/*}}}*/

	arcusFuture _coll_create(const string& cmd, const string& key, ARCUS_TYPE type,/*{{{*/
							 int exptime = 0, const arcusAttribute& attrs = default_attrs)
	{
		char buff[MAX_CMD_LEN];
		int bufflen;

		bufflen = snprintf(buff, sizeof(buff), "%s %s %d %d %d%s%s%s\r\n",
						   cmd.c_str(), key.c_str(), type, exptime, attrs.max_count,
						   attrs.get_ovfl_action(), attrs.readable?"":" unreadable",
						   attrs.noreply?" noreply":"");

		return add_op(cmd, buff, bufflen, &arcusMCNode::cb_coll_create);
	}
/*}}}*/

	virtual arcusFuture _bop_set(const string& cmd, const string& key, const arcusBKey& bkey, const arcusValue& value, const arcusEflag& eflag, const arcusAttribute& attrs = default_attrs)/*{{{*/
	{
		char buff[MAX_ELEMENT_SIZE + MAX_CMD_LEN];
		int bufflen;

		arcusData ret = transcoder->encode(value);

		if (attrs.create) {
			bufflen = snprintf(buff, sizeof(buff), "%s %s %s %s %d create %d %d %d%s%s%s%s\r\n",
							   cmd.c_str(), key.c_str(), bkey.to_str().c_str(), eflag.to_str().c_str(), ret.value.size(), 
							   attrs.flags, attrs.exptime, attrs.max_count,
							   attrs.get_ovfl_action(), attrs.readable?"":" unreadable",
							   attrs.noreply?" noreply":"", attrs.pipe?" pipe":"");
		}
		else {
			bufflen = snprintf(buff, sizeof(buff), "%s %s %s %s %d%s%s\r\n",
							   cmd.c_str(), key.c_str(), bkey.to_str().c_str(), eflag.to_str().c_str(), ret.value.size(), 
							   attrs.noreply?" noreply":"", attrs.pipe?" pipe":"");
		}

		memcpy(buff + bufflen, &ret.value[0], ret.value.size());
		bufflen += ret.value.size();
		memcpy(buff + bufflen, "\r\n", 2);
		bufflen += 2;

		return add_op("bop insert", buff, bufflen, &arcusMCNode::cb_coll_set);
	}
/*}}}*/


//
// callbacks
/////////////////////
	arcusValue cb_set()/*{{{*/
	{
		string line = handle.readline();
		if (line == "STORED") {
			return true;
		}

		if (line == "NOT_FOUND") {
			return false;
		}

		return false;
	}
/*}}}*/

	arcusValue cb_incr_decr()/*{{{*/
	{
		string line = handle.readline();
		int ret = 0;

		try {
			ret = lexical_cast<int>(line);
		}
		catch (bad_lexical_cast) {
			arcus_log("invalid recv: %s\n", line.c_str());
			return ARCUS_TYPE_EXCEPTION;
		}
		
		return ret;
	}
/*}}}*/

	arcusValue cb_get()/*{{{*/
	{
		string line = handle.readline();
		if (line.substr(0, 5) != "VALUE") {
			return ARCUS_PROTOCOL_EXCEPTION;
		}

		int idx = 0;
		string resp = tok(line, idx);
		string rkey = tok(line, idx);
		ARCUS_TYPE flags;
		int rlen;

		try {
			flags = TO_TYPE(lexical_cast<int>(tok(line, idx)));
			rlen = lexical_cast<int>(tok(line, idx));
		}
		catch (bad_lexical_cast) {
			arcus_log("invalid recv: %s\n", line.c_str());
			return ARCUS_PROTOCOL_EXCEPTION;
		}

		return decode_value(flags, rlen);
	}
/*}}}*/

	arcusValue cb_gets()/*{{{*/
	{
		string line = handle.readline();
		if (line.substr(0, 5) != "VALUE") {
			return ARCUS_PROTOCOL_EXCEPTION;
		}

		int idx = 0;
		string resp = tok(line, idx);
		string rkey = tok(line, idx);
		ARCUS_TYPE flags;
		int rlen;
		int cas_id;

		try {
			flags = TO_TYPE(lexical_cast<int>(tok(line, idx)));
			rlen = lexical_cast<int>(tok(line, idx));
			cas_id = lexical_cast<int>(tok(line, idx));
		}
		catch (bad_lexical_cast) {
			arcus_log("invalid recv: %s\n", line.c_str());
			return ARCUS_PROTOCOL_EXCEPTION;
		}

		return decode_value(flags, rlen);
	}
/*}}}*/

	arcusValue cb_coll_create()/*{{{*/
	{
		string line = handle.readline();
		if (line == "CREATED") {
			return true;
		}

		if (line == "EXISTS") {
			return ARCUS_COLLECTION_EXISTS_EXCEPTION;
		}

		return false;
	}
/*}}}*/

	arcusValue cb_coll_set()/*{{{*/
	{
		string line = handle.readline();
		if (line.substr(0, 8) == "RESPONSE") {
			int idx = 0;
			string dummy = tok(line, idx);
			int count;

			try {
				count = lexical_cast<int>(tok(line, idx));
			}
			catch (bad_lexical_cast) {
				arcus_log("invalid recv: %s\n", line.c_str());
				return ARCUS_PROTOCOL_EXCEPTION;
			}

			list<string> *ret = new list<string>();
			for (int i=0; i<count; i++) {
				line = handle.readline();
				ret->push_back(line);
			}

			handle.readline(); // read 'END'

			return ret;
		}

		if (line == "STORED") {
			return true;
		}

		if (line == "NOT_FOUND") {
			return false;
		}

		if (line == "TYPE_MISMATCH") {
			return ARCUS_COLLECTION_TYPE_EXCEPTION;
		}

		if (line == "OVERFLOWED") {
			return ARCUS_COLLECTION_OVERFLOW_EXCEPTION;
		}

		if (line == "OUT_OF_RANGE") {
			return ARCUS_COLLECTION_INDEX_EXCEPTION;
		}

		return false;
	}
/*}}}*/

	arcusValue cb_delete()/*{{{*/
	{
		string line = handle.readline();
		if (line.substr(0, 8) == "RESPONSE") {
			int idx = 0;
			string dummy = tok(line, idx);
			int count;

			try {
				count = lexical_cast<int>(tok(line, idx));
			}
			catch (bad_lexical_cast) {
				arcus_log("invalid recv: %s\n", line.c_str());
				return ARCUS_PROTOCOL_EXCEPTION;
			}

			list<string> *ret = new list<string>();
			for (int i=0; i<count; i++) {
				line = handle.readline();
				ret->push_back(line);
			}

			handle.readline(); // read 'END'

			return ret;
		}

		if (line == "DELETED") {
			return true;
		}

		if (line == "NOT_FOUND") {
			return true;
		}

		if (line == "TYPE_MISMATCH") {
			return ARCUS_COLLECTION_TYPE_EXCEPTION;
		}

		if (line == "OVERFLOWED") {
			return ARCUS_COLLECTION_OVERFLOW_EXCEPTION;
		}

		if (line == "OUT_OF_RANGE") {
			return ARCUS_COLLECTION_INDEX_EXCEPTION;
		}

		return false;
	}
/*}}}*/

	arcusValue cb_sop_exist()/*{{{*/
	{
		string line = handle.readline();
		if (line == "EXIST") {
			return true;
		}

		return false;
	}
/*}}}*/

	arcusValue cb_lop_get()/*{{{*/
	{
		return cb_coll_get(COLL_LOP);
	}
/*}}}*/

	arcusValue cb_sop_get()/*{{{*/
	{
		return cb_coll_get(COLL_SOP);
	}
/*}}}*/

	arcusValue cb_coll_get(COLLECTION_TYPE ctype)/*{{{*/
	{
		arcusValue ret;
		ret.cflags = ctype;
		bool bop_alloc = false;

		while (true) {
			string line = handle.readline();
			
			// handle error, end
			if (line.substr(0, 5) != "VALUE" and line.substr(0, 5) != "COUNT") {/*{{{*/
				if (line == "NOT_FOUND") {
					return ARCUS_COLLECTION_NOT_FOUND_EXCEPTION;
				}

				if (line == "TYPE_MISMATCH") {
					return ARCUS_COLLECTION_TYPE_EXCEPTION;
				}

				if (line == "UNREADABLE") {
					return ARCUS_COLLECTION_UNREADABLE_EXCEPTION;
				}

				if (line == "OUT_OF_RANGE" || line == "NOT_FOUND_ELEMENT") {
					return ARCUS_COLLECTION_INDEX_EXCEPTION;
				}

				if (line == "END") {
					break;
				}

				return ARCUS_PROTOCOL_EXCEPTION;
			}
/*}}}*/

			// parsing flags, count, alloc
			int idx = 0;/*{{{*/
			ARCUS_TYPE flags = FLAG_STRING;
			int count = 0;
			if (line.substr(0, 5) == "VALUE") {
				string dummy = tok(line, idx);
				flags = TO_TYPE(lexical_cast<int>(tok(line, idx)));
				count = lexical_cast<int>(tok(line, idx));

				ret.flags = flags;

				switch (ctype)
				{
				case COLL_LOP:
					switch(flags)
					{
					case FLAG_STRING:		ret.u.l_s = new list<string>(); break;
					case FLAG_BOOLEAN:		ret.u.l_b = new list<bool>(); break;
					case FLAG_INTEGER:		ret.u.l_i = new list<int>(); break;
					case FLAG_LONG:			ret.u.l_ll = new list<int64_t>(); break;
					case FLAG_DATE:			ret.u.l_t = new list<ptime>(); break;
					case FLAG_BYTE:			ret.u.l_c = new list<char>(); break;
					case FLAG_FLOAT:		ret.u.l_f = new list<float>(); break;
					case FLAG_DOUBLE:		ret.u.l_d = new list<double>(); break;
					case FLAG_BYTEARRAY:	ret.u.l_array = new list<vector<char> >(); break;
					}
					break;

				case COLL_SOP:
					switch(flags)
					{
					case FLAG_STRING:		ret.u.s_s = new std::set<string>(); break;
					case FLAG_BOOLEAN:		ret.u.s_b = new std::set<bool>(); break;
					case FLAG_INTEGER:		ret.u.s_i = new std::set<int>(); break;
					case FLAG_LONG:			ret.u.s_ll = new std::set<int64_t>(); break;
					case FLAG_DATE:			ret.u.s_t = new std::set<ptime>(); break;
					case FLAG_BYTE:			ret.u.s_c = new std::set<char>(); break;
					case FLAG_FLOAT:		ret.u.s_f = new std::set<float>(); break;
					case FLAG_DOUBLE:		ret.u.s_d = new std::set<double>(); break;
					case FLAG_BYTEARRAY:	ret.u.s_array = new std::set<vector<char> >(); break;
					}
					break;

				default:
					break;
				}
			}
/*}}}*/

			// decoding values
			for (int i=0; i<count; i++) {/*{{{*/
				char buff[MAX_ELEMENT_SIZE + 64]; // 64 for length & \r\n and margin
				int len = handle.readdata(buff, sizeof(buff));

				int length; // value length
				char *data;	// value pointer

				for (int i=0; i<len; i++) {
					if (buff[i] == ' ') {
						buff[i] = 0;
						data = &buff[i+1];
						length = lexical_cast<int>(buff);
						break;
					}

					if (i > 10) {
						return ARCUS_PROTOCOL_EXCEPTION;
					}
				}

				arcusValue value = transcoder->decode(flags, data, length);

				switch (ctype)
				{
				case COLL_LOP:
					switch(flags)
					{
					case FLAG_STRING:		ret.u.l_s->push_back(value.str); break;
					case FLAG_BOOLEAN:		ret.u.l_b->push_back(value.u.b); break;
					case FLAG_INTEGER:		ret.u.l_i->push_back(value.u.i); break;
					case FLAG_LONG:			ret.u.l_ll->push_back(value.u.ll); break;
					case FLAG_DATE:			ret.u.l_t->push_back(value.time); break;
					case FLAG_BYTE:			ret.u.l_c->push_back(value.u.c); break;
					case FLAG_FLOAT:		ret.u.l_f->push_back(value.u.f); break;
					case FLAG_DOUBLE:		ret.u.l_d->push_back(value.u.d); break;
					case FLAG_BYTEARRAY:	ret.u.l_array->push_back(value.vec); break;
					}
					break;

				case COLL_SOP:
					switch(flags)
					{
					case FLAG_STRING:		ret.u.s_s->insert(value.str); break;
					case FLAG_BOOLEAN:		ret.u.s_b->insert(value.u.b); break;
					case FLAG_INTEGER:		ret.u.s_i->insert(value.u.i); break;
					case FLAG_LONG:			ret.u.s_ll->insert(value.u.ll); break;
					case FLAG_DATE:			ret.u.s_t->insert(value.time); break;
					case FLAG_BYTE:			ret.u.s_c->insert(value.u.c); break;
					case FLAG_FLOAT:		ret.u.s_f->insert(value.u.f); break;
					case FLAG_DOUBLE:		ret.u.s_d->insert(value.u.d); break;
					case FLAG_BYTEARRAY:	ret.u.s_array->insert(value.vec); break;
					}
					break;
				}
			}
/*}}}*/
		}

		return ret;
	}
/*}}}*/

	arcusValue cb_bop_get()/*{{{*/
	{
		arcusValue ret;
		ret.cflags = COLL_BOP;
		bool bop_alloc = false;
		arcusBKey::ARCUS_BKEY_TYPE bkey_type = arcusBKey::BKEY_LONG;

		while (true) {
			string line = handle.readline();
			
			// handle error, end
			if (line.substr(0, 5) != "VALUE" and line.substr(0, 5) != "COUNT") {/*{{{*/
				if (line == "NOT_FOUND") {
					return ARCUS_COLLECTION_NOT_FOUND_EXCEPTION;
				}

				if (line == "TYPE_MISMATCH") {
					return ARCUS_COLLECTION_TYPE_EXCEPTION;
				}

				if (line == "UNREADABLE") {
					return ARCUS_COLLECTION_UNREADABLE_EXCEPTION;
				}

				if (line == "OUT_OF_RANGE" || line == "NOT_FOUND_ELEMENT") {
					return ARCUS_COLLECTION_INDEX_EXCEPTION;
				}

				if (line == "END") {
					break;
				}

				return ARCUS_PROTOCOL_EXCEPTION;
			}
/*}}}*/

			// parsing flag, count
			int idx = 0;/*{{{*/
			ARCUS_TYPE flags = FLAG_STRING;
			int count = 0;
			if (line.substr(0, 5) == "VALUE") {
				string dummy = tok(line, idx);
				flags = TO_TYPE(lexical_cast<int>(tok(line, idx)));
				count = lexical_cast<int>(tok(line, idx));
				ret.flags = flags;
			}
			else if (line.substr(0, 5) == "COUNT") {
				string dummy = tok(line, idx, "=");
				count = lexical_cast<int>(tok(line, idx)); 
				return count;
			}
/*}}}*/

			// decoding values
			for (int i=0; i<count; i++) {/*{{{*/
				char buff[MAX_ELEMENT_SIZE + 64]; // 64 for length & \r\n and margin
				int len = handle.readdata(buff, sizeof(buff));

				int length; // value length
				char *bkey, *eflag, *data;
				char* saveptr;

				bkey = strtok_r(buff, " ", &saveptr);
				if (bkey[0] == '0' && bkey[1] == 'x') {
					bkey_type = arcusBKey::BKEY_HEX;
				}
				
				eflag = strtok_r(NULL, " ", &saveptr);

				if (eflag[0] == '0' && eflag[1] == 'x') { // eflag exists
					char *cp = strtok_r(NULL, " ", &saveptr);
					length = lexical_cast<int>(cp);
					data = cp + strlen(cp) + 1;
				}
				else { // eflag not exists
					length = lexical_cast<int>(eflag);
					data = eflag + strlen(eflag) + 1;
					eflag = "";
				}

				// bop alloc
				if (bop_alloc == false)/*{{{*/
				{
					bop_alloc = true;

					if (bkey_type == arcusBKey::BKEY_LONG) {
						switch(flags)
						{
						case FLAG_STRING:		ret.u.m_l_s = new map<uint64_t, arcusBopItem<string> >(); break;
						case FLAG_BOOLEAN:		ret.u.m_l_b = new map<uint64_t, arcusBopItem<bool> >(); break;
						case FLAG_INTEGER:		ret.u.m_l_i = new map<uint64_t, arcusBopItem<int> >(); break;
						case FLAG_LONG:			ret.u.m_l_ll = new map<uint64_t, arcusBopItem<int64_t> >(); break;
						case FLAG_DATE:			ret.u.m_l_t = new map<uint64_t, arcusBopItem<ptime> >(); break;
						case FLAG_BYTE:			ret.u.m_l_c = new map<uint64_t, arcusBopItem<char> >(); break;
						case FLAG_FLOAT:		ret.u.m_l_f = new map<uint64_t, arcusBopItem<float> >(); break;
						case FLAG_DOUBLE:		ret.u.m_l_d = new map<uint64_t, arcusBopItem<double> >(); break;
						case FLAG_BYTEARRAY:	ret.u.m_l_array = new map<uint64_t, arcusBopItem<vector<char> > >(); break;
						}
					}
					else {
						switch(flags)
						{
						case FLAG_STRING:		ret.u.m_v_s = new map<vector<unsigned char>, arcusBopItem<string> >(); break;
						case FLAG_BOOLEAN:		ret.u.m_v_b = new map<vector<unsigned char>, arcusBopItem<bool> >(); break;
						case FLAG_INTEGER:		ret.u.m_v_i = new map<vector<unsigned char>, arcusBopItem<int> >(); break;
						case FLAG_LONG:			ret.u.m_v_ll = new map<vector<unsigned char>, arcusBopItem<int64_t> >(); break;
						case FLAG_DATE:			ret.u.m_v_t = new map<vector<unsigned char>, arcusBopItem<ptime> >(); break;
						case FLAG_BYTE:			ret.u.m_v_c = new map<vector<unsigned char>, arcusBopItem<char> >(); break;
						case FLAG_FLOAT:		ret.u.m_v_f = new map<vector<unsigned char>, arcusBopItem<float> >(); break;
						case FLAG_DOUBLE:		ret.u.m_v_d = new map<vector<unsigned char>, arcusBopItem<double> >(); break;
						case FLAG_BYTEARRAY:	ret.u.m_v_array = new map<vector<unsigned char>, arcusBopItem<vector<char> > >(); break;
						}
					}
				}/*}}}*/


				// push value
				arcusValue v = transcoder->decode(flags, data, length);

				if (bkey_type == arcusBKey::BKEY_LONG) {
					uint64_t key = lexical_cast<uint64_t>(bkey);

					switch(flags)
					{
					case FLAG_STRING:		(*(ret.u.m_l_s))[key] = arcusBopItem<string>(v.str, util.hstr2vec(eflag)); break;
					case FLAG_BOOLEAN:		(*(ret.u.m_l_b))[key] = arcusBopItem<bool>(v.u.b, util.hstr2vec(eflag)); break;
					case FLAG_INTEGER:		(*(ret.u.m_l_i))[key] = arcusBopItem<int>(v.u.i, util.hstr2vec(eflag)); break;
					case FLAG_LONG:			(*(ret.u.m_l_ll))[key] = arcusBopItem<int64_t>(v.u.ll, util.hstr2vec(eflag)); break;
					case FLAG_DATE:			(*(ret.u.m_l_t))[key] = arcusBopItem<ptime>(v.time, util.hstr2vec(eflag)); break;
					case FLAG_BYTE:			(*(ret.u.m_l_c))[key] = arcusBopItem<char>(v.u.c, util.hstr2vec(eflag)); break;
					case FLAG_FLOAT:		(*(ret.u.m_l_f))[key] = arcusBopItem<float>(v.u.f, util.hstr2vec(eflag)); break;
					case FLAG_DOUBLE:		(*(ret.u.m_l_d))[key] = arcusBopItem<double>(v.u.d, util.hstr2vec(eflag)); break;
					case FLAG_BYTEARRAY:	(*(ret.u.m_l_array))[key] = arcusBopItem<vector<char> >(v.vec, util.hstr2vec(eflag)); break;
					}
				}
				else {
					vector<unsigned char> key = util.hstr2vec(bkey);
					
					switch(flags)
					{
					case FLAG_STRING:		(*(ret.u.m_v_s))[key] = arcusBopItem<string>(v.str, util.hstr2vec(eflag)); break;
					case FLAG_BOOLEAN:		(*(ret.u.m_v_b))[key] = arcusBopItem<bool>(v.u.b, util.hstr2vec(eflag)); break;
					case FLAG_INTEGER:		(*(ret.u.m_v_i))[key] = arcusBopItem<int>(v.u.i, util.hstr2vec(eflag)); break;
					case FLAG_LONG:			(*(ret.u.m_v_ll))[key] = arcusBopItem<int64_t>(v.u.ll, util.hstr2vec(eflag)); break;
					case FLAG_DATE:			(*(ret.u.m_v_t))[key] = arcusBopItem<ptime>(v.time, util.hstr2vec(eflag)); break;
					case FLAG_BYTE:			(*(ret.u.m_v_c))[key] = arcusBopItem<char>(v.u.c, util.hstr2vec(eflag)); break;
					case FLAG_FLOAT:		(*(ret.u.m_v_f))[key] = arcusBopItem<float>(v.u.f, util.hstr2vec(eflag)); break;
					case FLAG_DOUBLE:		(*(ret.u.m_v_d))[key] = arcusBopItem<double>(v.u.d, util.hstr2vec(eflag)); break;
					case FLAG_BYTEARRAY:	(*(ret.u.m_v_array))[key] = arcusBopItem<vector<char> >(v.vec, util.hstr2vec(eflag)); break;
					}
				}
			}
/*}}}*/
		}

		return ret;
	}
/*}}}*/

	arcusValue cb_bop_mget()/*{{{*/
	{
		arcusValue ret;
		ret.cflags = COLL_BOP;
		bool bop_alloc = false;
		arcusBKey::ARCUS_BKEY_TYPE bkey_type = arcusBKey::BKEY_LONG;

		while (true) {
			int idx, count;
			string line = handle.readline();
			
			// handle error, end
			idx = 0;
			count = 0;

			// handle error, end
			if (line.substr(0, 5) != "VALUE" and line.substr(0, 5) != "COUNT") {
				// TODO: check return
				return ret;
			}

			// parsing flag, count
			idx = 0;/*{{{*/
			count = 0;
			string KEY;
			ARCUS_TYPE flags = FLAG_STRING;
			if (line.substr(0, 5) == "VALUE") {
				string dummy = tok(line, idx);
				KEY = tok(line, idx);
				string status = tok(line, idx);

				if (status == "OK" || status == "TRIMMED") {
					flags = TO_TYPE(lexical_cast<int>(tok(line, idx)));
					count = lexical_cast<int>(tok(line, idx));
				}
				else if (status == "NOT_FOUND") {
					ret.missed_keys.push_back(KEY);
					continue;
				}
				else {
					continue;

				}

				ret.flags = flags;
			}
/*}}}*/

			// decoding values
			for (int i=0; i<count; i++) {/*{{{*/
				char buff[MAX_ELEMENT_SIZE + 64]; // 64 for length & \r\n and margin
				int len = handle.readdata(buff, sizeof(buff));

				int length; // value length
				char *element, *bkey, *eflag, *data;
				char* saveptr;

				element = strtok_r(buff, " ", &saveptr);
				bkey = strtok_r(NULL, " ", &saveptr);
				if (bkey[0] == '0' && bkey[1] == 'x') {
					bkey_type = arcusBKey::BKEY_HEX;
				}
				
				eflag = strtok_r(NULL, " ", &saveptr);

				if (eflag[0] == '0' && eflag[1] == 'x') { // eflag exists
					char *cp = strtok_r(NULL, " ", &saveptr);
					length = lexical_cast<int>(cp);
					data = cp + strlen(cp) + 1;
				}
				else { // eflag not exists
					length = lexical_cast<int>(eflag);
					data = eflag + strlen(eflag) + 1;
					eflag = NULL;
				}

				// bop alloc
				if (bop_alloc == false)/*{{{*/
				{
					bop_alloc = true;

					if (bkey_type == arcusBKey::BKEY_LONG) {
						switch(flags)
						{
						case FLAG_STRING:		ret.u.mget_l_s = new map<string, map<uint64_t, arcusBopItem<string> > >(); break;
						case FLAG_BOOLEAN:		ret.u.mget_l_b = new map<string, map<uint64_t, arcusBopItem<bool> > >(); break;
						case FLAG_INTEGER:		ret.u.mget_l_i = new map<string, map<uint64_t, arcusBopItem<int> > >(); break;
						case FLAG_LONG:			ret.u.mget_l_ll = new map<string, map<uint64_t, arcusBopItem<int64_t> > >(); break;
						case FLAG_DATE:			ret.u.mget_l_t = new map<string, map<uint64_t, arcusBopItem<ptime> > >(); break;
						case FLAG_BYTE:			ret.u.mget_l_c = new map<string, map<uint64_t, arcusBopItem<char> > >(); break;
						case FLAG_FLOAT:		ret.u.mget_l_f = new map<string, map<uint64_t, arcusBopItem<float> > >(); break;
						case FLAG_DOUBLE:		ret.u.mget_l_d = new map<string, map<uint64_t, arcusBopItem<double> > >(); break;
						case FLAG_BYTEARRAY:	ret.u.mget_l_array = new map<string, map<uint64_t, arcusBopItem<vector<char> > > >(); break;
						}
					}
					else {
						switch(flags)
						{
						case FLAG_STRING:		ret.u.mget_v_s = new map<string, map<vector<unsigned char>, arcusBopItem<string> > >(); break;
						case FLAG_BOOLEAN:		ret.u.mget_v_b = new map<string, map<vector<unsigned char>, arcusBopItem<bool> > >(); break;
						case FLAG_INTEGER:		ret.u.mget_v_i = new map<string, map<vector<unsigned char>, arcusBopItem<int> > >(); break;
						case FLAG_LONG:			ret.u.mget_v_ll = new map<string, map<vector<unsigned char>, arcusBopItem<int64_t> > >(); break;
						case FLAG_DATE:			ret.u.mget_v_t = new map<string, map<vector<unsigned char>, arcusBopItem<ptime> > >(); break;
						case FLAG_BYTE:			ret.u.mget_v_c = new map<string, map<vector<unsigned char>, arcusBopItem<char> > >(); break;
						case FLAG_FLOAT:		ret.u.mget_v_f = new map<string, map<vector<unsigned char>, arcusBopItem<float> > >(); break;
						case FLAG_DOUBLE:		ret.u.mget_v_d = new map<string, map<vector<unsigned char>, arcusBopItem<double> > >(); break;
						case FLAG_BYTEARRAY:	ret.u.mget_v_array = new map<string, map<vector<unsigned char>, arcusBopItem<vector<char> > > >(); break;
						}
					}
				}/*}}}*/


				// push value
				arcusValue v = transcoder->decode(flags, data, length);

				if (bkey_type == arcusBKey::BKEY_LONG) {
					uint64_t key = lexical_cast<uint64_t>(bkey);

					switch(flags)
					{
					case FLAG_STRING:		(*(ret.u.mget_l_s))[KEY][key] = arcusBopItem<string>(v.str, util.hstr2vec(eflag)); break;
					case FLAG_BOOLEAN:		(*(ret.u.mget_l_b))[KEY][key] = arcusBopItem<bool>(v.u.b, util.hstr2vec(eflag)); break;
					case FLAG_INTEGER:		(*(ret.u.mget_l_i))[KEY][key] = arcusBopItem<int>(v.u.i, util.hstr2vec(eflag)); break;
					case FLAG_LONG:			(*(ret.u.mget_l_ll))[KEY][key] = arcusBopItem<int64_t>(v.u.ll, util.hstr2vec(eflag)); break;
					case FLAG_DATE:			(*(ret.u.mget_l_t))[KEY][key] = arcusBopItem<ptime>(v.time, util.hstr2vec(eflag)); break;
					case FLAG_BYTE:			(*(ret.u.mget_l_c))[KEY][key] = arcusBopItem<char>(v.u.c, util.hstr2vec(eflag)); break;
					case FLAG_FLOAT:		(*(ret.u.mget_l_f))[KEY][key] = arcusBopItem<float>(v.u.f, util.hstr2vec(eflag)); break;
					case FLAG_DOUBLE:		(*(ret.u.mget_l_d))[KEY][key] = arcusBopItem<double>(v.u.d, util.hstr2vec(eflag)); break;
					case FLAG_BYTEARRAY:	(*(ret.u.mget_l_array))[KEY][key] = arcusBopItem<vector<char> >(v.vec, util.hstr2vec(eflag)); break;
					}
				}
				else {
					vector<unsigned char> key = util.hstr2vec(bkey);
					
					switch(flags)
					{
					case FLAG_STRING:		(*(ret.u.mget_v_s))[KEY][key] = arcusBopItem<string>(v.str, util.hstr2vec(eflag)); break;
					case FLAG_BOOLEAN:		(*(ret.u.mget_v_b))[KEY][key] = arcusBopItem<bool>(v.u.b, util.hstr2vec(eflag)); break;
					case FLAG_INTEGER:		(*(ret.u.mget_v_i))[KEY][key] = arcusBopItem<int>(v.u.i, util.hstr2vec(eflag)); break;
					case FLAG_LONG:			(*(ret.u.mget_v_ll))[KEY][key] = arcusBopItem<int64_t>(v.u.ll, util.hstr2vec(eflag)); break;
					case FLAG_DATE:			(*(ret.u.mget_v_t))[KEY][key] = arcusBopItem<ptime>(v.time, util.hstr2vec(eflag)); break;
					case FLAG_BYTE:			(*(ret.u.mget_v_c))[KEY][key] = arcusBopItem<char>(v.u.c, util.hstr2vec(eflag)); break;
					case FLAG_FLOAT:		(*(ret.u.mget_v_f))[KEY][key] = arcusBopItem<float>(v.u.f, util.hstr2vec(eflag)); break;
					case FLAG_DOUBLE:		(*(ret.u.mget_v_d))[KEY][key] = arcusBopItem<double>(v.u.d, util.hstr2vec(eflag)); break;
					case FLAG_BYTEARRAY:	(*(ret.u.mget_v_array))[KEY][key] = arcusBopItem<vector<char> >(v.vec, util.hstr2vec(eflag)); break;
					}
				}
			}
/*}}}*/
		}

		return ret;
	}
/*}}}*/

	arcusValue cb_bop_smget()/*{{{*/
	{
		arcusValue ret;
		ret.cflags = COLL_BOP;
		bool bop_alloc = false;
		arcusBKey::ARCUS_BKEY_TYPE bkey_type = arcusBKey::BKEY_LONG;

		while (true) {
			int idx, count;
			string line = handle.readline();
			
			// handle error, end
			idx = 0;
			count = 0;
			if (line.substr(0, 11) == "MISSED_KEYS") {
				string dummy = tok(line, idx);
				count = lexical_cast<int>(tok(line, idx));

				for (int i=0; i<count; i++) {
					line = handle.readline();
					ret.missed_keys.push_back(line);
				}
				
				continue;
			}

			// handle error, end
			if (line.substr(0, 5) != "VALUE" and line.substr(0, 5) != "COUNT") {
				// TODO: check return
				return ret;
			}

			// parsing flag, count
			idx = 0;/*{{{*/
			count = 0;
			ARCUS_TYPE flags = FLAG_STRING;
			if (line.substr(0, 5) == "VALUE") {
				string dummy = tok(line, idx);
				count = lexical_cast<int>(tok(line, idx));
			}
/*}}}*/

			// decoding values
			for (int i=0; i<count; i++) {/*{{{*/
				char buff[MAX_ELEMENT_SIZE + 64]; // 64 for length & \r\n and margin
				int len = handle.readdata(buff, sizeof(buff));

				int length; // value length
				char *KEY, *bkey, *eflag, *data;
				char* saveptr;

				KEY = strtok_r(buff, " ", &saveptr);
				flags = TO_TYPE(lexical_cast<int>(strtok_r(NULL, " ", &saveptr)));
				bkey = strtok_r(NULL, " ", &saveptr);
				if (bkey[0] == '0' && bkey[1] == 'x') {
					bkey_type = arcusBKey::BKEY_HEX;
				}

				eflag = strtok_r(NULL, " ", &saveptr);
				if (eflag[0] == '0' && eflag[1] == 'x') { // eflag exists
					char *cp = strtok_r(NULL, " ", &saveptr);
					length = lexical_cast<int>(cp);
					data = cp + strlen(cp) + 1;
				}
				else { // eflag not exists
					length = lexical_cast<int>(eflag);
					data = eflag + strlen(eflag) + 1;
					eflag = NULL;
				}

				// bop alloc
				if (bop_alloc == false)/*{{{*/
				{
					bop_alloc = true;

					if (bkey_type == arcusBKey::BKEY_LONG) {
						switch(flags)
						{
						case FLAG_STRING:		ret.u.mm_l_s = new multimap<uint64_t, arcusBopItem<string> >(); break;
						case FLAG_BOOLEAN:		ret.u.mm_l_b = new multimap<uint64_t, arcusBopItem<bool> >(); break;
						case FLAG_INTEGER:		ret.u.mm_l_i = new multimap<uint64_t, arcusBopItem<int> >(); break;
						case FLAG_LONG:			ret.u.mm_l_ll = new multimap<uint64_t, arcusBopItem<int64_t> >(); break;
						case FLAG_DATE:			ret.u.mm_l_t = new multimap<uint64_t, arcusBopItem<ptime> >(); break;
						case FLAG_BYTE:			ret.u.mm_l_c = new multimap<uint64_t, arcusBopItem<char> >(); break;
						case FLAG_FLOAT:		ret.u.mm_l_f = new multimap<uint64_t, arcusBopItem<float> >(); break;
						case FLAG_DOUBLE:		ret.u.mm_l_d = new multimap<uint64_t, arcusBopItem<double> >(); break;
						case FLAG_BYTEARRAY:	ret.u.mm_l_array = new multimap<uint64_t, arcusBopItem<vector<char> > >(); break;
						}
					}
					else {
						switch(flags)
						{
						case FLAG_STRING:		ret.u.mm_v_s = new multimap<vector<unsigned char>, arcusBopItem<string> >(); break;
						case FLAG_BOOLEAN:		ret.u.mm_v_b = new multimap<vector<unsigned char>, arcusBopItem<bool> >(); break;
						case FLAG_INTEGER:		ret.u.mm_v_i = new multimap<vector<unsigned char>, arcusBopItem<int> >(); break;
						case FLAG_LONG:			ret.u.mm_v_ll = new multimap<vector<unsigned char>, arcusBopItem<int64_t> >(); break;
						case FLAG_DATE:			ret.u.mm_v_t = new multimap<vector<unsigned char>, arcusBopItem<ptime> >(); break;
						case FLAG_BYTE:			ret.u.mm_v_c = new multimap<vector<unsigned char>, arcusBopItem<char> >(); break;
						case FLAG_FLOAT:		ret.u.mm_v_f = new multimap<vector<unsigned char>, arcusBopItem<float> >(); break;
						case FLAG_DOUBLE:		ret.u.mm_v_d = new multimap<vector<unsigned char>, arcusBopItem<double> >(); break;
						case FLAG_BYTEARRAY:	ret.u.mm_v_array = new multimap<vector<unsigned char>, arcusBopItem<vector<char> > >(); break;
						}
					}
				}/*}}}*/


				// push value
				arcusValue v = transcoder->decode(flags, data, length);

				if (bkey_type == arcusBKey::BKEY_LONG) {
					uint64_t key = lexical_cast<uint64_t>(bkey);

					switch(flags)
					{
					case FLAG_STRING:		ret.u.mm_l_s->insert(pair<uint64_t, arcusBopItem<string> >(key,arcusBopItem<string>(v.str, util.hstr2vec(eflag), KEY))); break;
					case FLAG_BOOLEAN:		ret.u.mm_l_b->insert(pair<uint64_t, arcusBopItem<bool> >(key,arcusBopItem<bool>(v.u.b, util.hstr2vec(eflag), KEY))); break;
					case FLAG_INTEGER:		ret.u.mm_l_i->insert(pair<uint64_t, arcusBopItem<int> >(key,arcusBopItem<int>(v.u.i, util.hstr2vec(eflag), KEY))); break;
					case FLAG_LONG:			ret.u.mm_l_ll->insert(pair<uint64_t, arcusBopItem<int64_t> >(key,arcusBopItem<int64_t>(v.u.ll, util.hstr2vec(eflag), KEY))); break;
					case FLAG_DATE:			ret.u.mm_l_t->insert(pair<uint64_t, arcusBopItem<ptime> >(key,arcusBopItem<ptime>(v.time, util.hstr2vec(eflag), KEY))); break;
					case FLAG_BYTE:			ret.u.mm_l_c->insert(pair<uint64_t, arcusBopItem<char> >(key,arcusBopItem<char>(v.u.c, util.hstr2vec(eflag), KEY))); break;
					case FLAG_FLOAT:		ret.u.mm_l_f->insert(pair<uint64_t, arcusBopItem<float> >(key,arcusBopItem<float>(v.u.f, util.hstr2vec(eflag), KEY))); break;
					case FLAG_DOUBLE:		ret.u.mm_l_d->insert(pair<uint64_t, arcusBopItem<double> >(key,arcusBopItem<double>(v.u.d, util.hstr2vec(eflag), KEY))); break;
					case FLAG_BYTEARRAY:	ret.u.mm_l_array->insert(pair<uint64_t, arcusBopItem<vector<char> > >(key,arcusBopItem<vector<char> >(v.vec, util.hstr2vec(eflag), KEY))); break;
					}
				}
				else {
					vector<unsigned char> key = util.hstr2vec(bkey);
					
					switch(flags)
					{
					case FLAG_STRING:		ret.u.mm_v_s->insert(pair<vector<unsigned char>, arcusBopItem<string> >(key,arcusBopItem<string>(v.str, util.hstr2vec(eflag), KEY))); break;
					case FLAG_BOOLEAN:		ret.u.mm_v_b->insert(pair<vector<unsigned char>, arcusBopItem<bool> >(key,arcusBopItem<bool>(v.u.b, util.hstr2vec(eflag), KEY))); break;
					case FLAG_INTEGER:		ret.u.mm_v_i->insert(pair<vector<unsigned char>, arcusBopItem<int> >(key,arcusBopItem<int>(v.u.i, util.hstr2vec(eflag), KEY))); break;
					case FLAG_LONG:			ret.u.mm_v_ll->insert(pair<vector<unsigned char>, arcusBopItem<int64_t> >(key,arcusBopItem<int64_t>(v.u.ll, util.hstr2vec(eflag), KEY))); break;
					case FLAG_DATE:			ret.u.mm_v_t->insert(pair<vector<unsigned char>, arcusBopItem<ptime> >(key,arcusBopItem<ptime>(v.time, util.hstr2vec(eflag), KEY))); break;
					case FLAG_BYTE:			ret.u.mm_v_c->insert(pair<vector<unsigned char>, arcusBopItem<char> >(key,arcusBopItem<char>(v.u.c, util.hstr2vec(eflag), KEY))); break;
					case FLAG_FLOAT:		ret.u.mm_v_f->insert(pair<vector<unsigned char>, arcusBopItem<float> >(key,arcusBopItem<float>(v.u.f, util.hstr2vec(eflag), KEY))); break;
					case FLAG_DOUBLE:		ret.u.mm_v_d->insert(pair<vector<unsigned char>, arcusBopItem<double> >(key,arcusBopItem<double>(v.u.d, util.hstr2vec(eflag), KEY))); break;
					case FLAG_BYTEARRAY:	ret.u.mm_v_array->insert(pair<vector<unsigned char>, arcusBopItem<vector<char> > >(key,arcusBopItem<vector<char> >(v.vec, util.hstr2vec(eflag), KEY))); break;
					}
				}
			}
/*}}}*/
		}

		return ret;
	}
/*}}}*/

	arcusValue decode_value(ARCUS_TYPE flags, int rlen)/*{{{*/
	{
		char _buff[4096];
		char *buff = _buff;

		if (rlen > sizeof(_buff)) {
			buff = (char*)malloc(rlen);
		}

		rlen += 2;
		int ret = handle.recv(buff, rlen);

		if (ret != rlen) {
			if (buff != _buff) {
				free(buff);
			}
			return ARCUS_PROTOCOL_EXCEPTION;
		}

		if (handle.readline() != "END") {
			if (buff != _buff) {
				free(buff);
			}
			return ARCUS_PROTOCOL_EXCEPTION;
		}

		buff[ret-2] = 0; // strip \r\n
		arcusValue value = transcoder->decode(flags, buff, ret - 2);
		if (buff != _buff) {
			free(buff);
		}

		return value;
	}
/*}}}*/

	string tok(const string& src, int& start, const string& match = " ")/*{{{*/
	{
		int idx = src.find_first_of(match, start);
		if (idx < start) {
			idx = src.length();
		}

		int org_start = start;
		start = idx + match.length();

		return src.substr(org_start, idx-org_start);
	}
/*}}}*/
	

private:
	list<arcusFuture> ops;
	arcusMCConnection handle;
	arcusTranscoder* transcoder;

	pthread_mutex_t mutex;
};

#define EPOLL_SIZE 1024

class arcusMCPoll : public threadClass/*{{{*/
{
public:
	static arcusMCPoll* handle()
	{
		static arcusMCPoll* p = NULL;
		
		if (p == NULL) {
			p = new arcusMCPoll();
		}

		return p;
	}

	~arcusMCPoll()
	{
		close(efd);
		free(events);
	}

	virtual void run()
	{
		arcus_log("Poll: start\n");
		while(true) {

			int ret = epoll_wait(efd, events, EPOLL_SIZE, 20000);

			if (arcusSystem::shutdown() == true) {
				return;
			}

			arcus_log("Poll: ret - %d\n", ret);

			for (int i=0; i<ret; i++) {
				if (events[i].events & EPOLLIN) {
					arcus_log("Poll: read fd:%d\n", events[i].data.fd);
					arcusMCNode* node = sock_node_map[events[i].data.fd];
					node->do_op();
				}

				if (events[i].events & EPOLLHUP) {
					arcus_log("Poll: hup fd:%d\n", events[i].data.fd);
					epoll_ctl(efd, EPOLL_CTL_DEL, events[i].data.fd, events);
					arcusMCNode* node = sock_node_map[events[i].data.fd];
					node->disconnect();
					sock_node_map.erase(events[i].data.fd);
				}
			}
		}
	}

	void register_node(arcusMCNode* node)
	{
		struct epoll_event ev;

		ev.events = EPOLLIN | EPOLLHUP;
		ev.data.fd = node->get_fileno();

		sock_node_map[ev.data.fd] = node;

		arcus_log("Poll: register fd:%d\n", ev.data.fd);
		epoll_ctl(efd, EPOLL_CTL_ADD, ev.data.fd, &ev);
	}

private:
	arcusMCPoll()
	{
		efd = epoll_create(EPOLL_SIZE);
		events = (struct epoll_event *)malloc(sizeof(struct epoll_event) * EPOLL_SIZE);
	}

	map<int, arcusMCNode*> sock_node_map;

	int efd;
	struct epoll_event *events;
};
/*}}}*/

class arcusMCNodeAllocator : public arcusNodeAllocator/*{{{*/
{
public:
	arcusMCNodeAllocator(arcusTranscoder* tc)
	{
		transcoder = tc;
		arcusMCWorker::handle()->start();
		arcusMCPoll::handle()->start();
	}

	virtual arcusNode* alloc(const string& addr, const string& name)
	{
		arcusMCNode* node =  new arcusMCNode(addr, name, transcoder);
		arcusMCPoll::handle()->register_node(node);
		return node;
	}

private:
	arcusTranscoder* transcoder;
};
/*}}}*/

template <typename T>
void process_request_helper(arcusMCNode* node, const char* req, int rlen)
{
	node->process_request(req, rlen);
}


#endif


