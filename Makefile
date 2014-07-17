

all:
	g++ -g -c test.cpp -I/usr/local/include -I/usr/local/include/zookeeper
	g++ -o test test.o -lzookeeper_mt -lhashkit

clean:
	rm -rf *.o
	rm -rf test
	
