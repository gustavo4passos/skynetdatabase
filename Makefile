CC = g++
CFLAGS = -std=c++11 -pedantic -Wall 

all:
	$(CC) -g src/database.cpp src/main.cpp -o bin/main $(CFLAGS)