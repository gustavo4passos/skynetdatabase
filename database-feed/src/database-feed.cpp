#include "database.h"

#include <iostream>
#include <fstream>
#include <string>

int main(int argc, char* args[])
{
    using namespace std;

    if(argc < 2)
    {
        cerr << "Error: Missing file name." << endl;
        return 0;
    }

    ifstream file;
    file.open(args[1]);

    if(!file.is_open())
    {
        cerr << "Error: Unable to open file: " << args[1] << std::endl; 
        return 0;
    }

    Database* db = new Database();
    std::string line;
    while(getline(file, line))
    {
        
        size_t commaPos = line.find(',');
        string key = line.substr(0, commaPos);
        if(key == "ssec")
        {
            int a;
            cout << a << endl;
        }
        string value = line.substr(commaPos + 1, line.size() - commaPos - 1);
        db->InsertEntry(key.c_str(), value.c_str());
    }

    delete db;
    db = nullptr;

    return 0;
}