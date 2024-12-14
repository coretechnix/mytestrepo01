#!/bin/bash

#g++ -m64 -g -std=c++17 Sys.Comm.pb.cc System.Comm.Event.Distr.pb.cc System.Comm.Schedule.Distr.pb.cc server_demo2.cpp -o server_demo.bin $(pkg-config --libs --cflags protobuf) -lstdc++fs
#g++ -m64 -g -std=c++17 Sys.Comm.pb.cc System.Comm.Event.Distr.pb.cc System.Comm.Schedule.Distr.pb.cc client_demo2.cpp -o client_demo.bin $(pkg-config --libs --cflags protobuf) -lstdc++fs
g++ -std=c++17 Sys.Comm.pb.cc System.Comm.Event.Distr.pb.cc System.Comm.Schedule.Distr.pb.cc server_demo2.cpp -o server_demo.bin $(pkg-config --libs --cflags protobuf) -lstdc++fs
g++ -std=c++17 Sys.Comm.pb.cc System.Comm.Event.Distr.pb.cc System.Comm.Schedule.Distr.pb.cc client_demo2.cpp -o client_demo.bin $(pkg-config --libs --cflags protobuf) -lstdc++fs
