#include<iostream>
#include "MockPDServer.h"

int main(int argv, char** args)
{
    std::vector<std::string> addrs;
    for (int i = 1; i < argv; i++)
    {
        std::cout<<args[i]<<std::endl;
        addrs.push_back(args[i]);
    }
    pingcap::pd::mock::RunPDServer(addrs);
    return 0;
}
