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

    int InsertEntry(const char key[21], const char value[51]);

    // Stores entry into outValue if entry is found in the database.
    // Returns 1, if entry is found.
    // Returns 0 otherwise, and keeps outValue unchanged
    int GetEntry(const char key[21], char outValue[21]);
    int DeleteEntry(const char* key);

private:
    std::fstream m_mainDataFile;

    bool OpenMainDataFile();
    void CreateHeaderFile();
    void CreateMainDataFile();
    unsigned CalcHash(const char key[21]);
    unsigned PowerOfTwo(int exponent);
    unsigned CalcIndexOffset(int index);
    unsigned CalcOffsetFromKey(const char key[21]);
    std::streampos FindFirstEmptySlot();
    void WriteEntryAtPosition(
        const char key[21], 
        const char value[51], 
        const std::streampos& pos
    );

    unsigned m_level;
    unsigned m_next;
    unsigned m_numberOfEntries;
    unsigned m_numberOfPages;
    
    const float MIN = 0.3f;
    const float MAX = 0.8f;
    const unsigned N = 11;
    const unsigned ENTRIES_PER_PAGE = 11;
    const char* MAIN_DATA_FILE_NAME = "data.dat";
    const unsigned HEADER_SIZE = sizeof(Header);
    const unsigned ENTRY_SIZE = sizeof(Entry);
    const unsigned INDEX_HEADER_SIZE = sizeof(IndexHeader);
    const unsigned INDEX_SIZE = INDEX_HEADER_SIZE + ENTRIES_PER_PAGE * ENTRY_SIZE;

    #pragma pack(push, 1)
    struct Header
    {
        unsigned N;
        unsigned level;
        unsigned numberOfPages;
        unsigned numberOfEntires;
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