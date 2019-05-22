#pragma once

#include <fstream>

#pragma pack(push, 1)
struct Entry 
{
    const char key[21];
    const char value[51];
};
#pragma pack(pop)

class Database {
public:
    explicit Database();
    ~Database();

    int InsertEntry(const char* key);
    int DeleteEntry(const char* key);

private:
    std::fstream m_mainDataFile;

    bool OpenMainDataFile();
    void CreateHeaderFile();
    void CreateMainDataFile();

    unsigned m_level;
    unsigned m_next;
    
    const float MIN = 0.3f;
    const float MAX = 0.8f;
    const unsigned N = 11;
    const unsigned ENTRIES_PER_PAGE = 11;
    const char* MAIN_DATA_FILE_NAME = "data.dat";
    const unsigned HEADER_SIZE = sizeof(Header);
    const unsigned INDEX_HEADER_SIZE = sizeof(IndexHeader);

    #pragma pack(push, 1)
    struct Header
    {
        unsigned N;
        unsigned level;
        unsigned numberOfPages;
        unsigned next;
    };
    #pragma pack(pop)

    #pragma pack(push, 1)
    struct IndexHeader
    {
        unsigned numberOfExtensions;
        unsigned numberOfEntries;
    };
    #pragma pack(pop)

};