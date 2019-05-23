/*  Universidade Federal da Bahia      */
/*  Estrutura de Dados e Algoritmos II */
/*  Alisson Souza                      */
/*  Gustavo Passos                     */
/*  Kaio Carvalho                      */

#pragma once

#include <fstream>
#include <vector>

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
    std::vector<std::fstream> m_extensionPages;

    // Tries to open the main data file.
    // Returns true if the file was successfully opened.
    // Returns false otherwise.
    bool OpenMainDataFile();

    // Creates the main data file for the first time
    // using default values based on the parameters
    // defined below (N and entries per page). 
    // The main data file contains the main header
    // (descbried by the Header sctructure) and each
    // index first page.
    // Each index is composed by an index header 
    // (described the the IndexHeader structure)
    // and the entries. The ammount of entries
    // per index is defined by the ENTRIES_PER_PAGE constant
    void CreateMainDataFile();

    // Store the database current state to the main data file
    void UpdateMainHeader();

    unsigned CalcHash(const char key[21]);

    unsigned TwoToThePower(int exponent);

    // Returns the position in the file of an index
    unsigned CalcIndexOffset(int index, bool isMainFile = true);

    // Returns the position of the file of the index where
    // key[21] hashes to
    unsigned CalcIndexOffsetFromKey(const char key[21]);

    // Finds the file offset which represents the first empty slot 
    // of an index
    std::streampos FindFirstEmptySlot();
    void WriteEntryAtPosition(
        const char key[21], 
        const char value[51], 
        const std::streampos& pos
    );

    void SplitPage(unsigned page);
    void DistributeEntries(unsigned page);

    unsigned m_level;
    unsigned m_next;
    unsigned m_numberOfEntries;
    unsigned m_numberOfPages;
    
    const float MIN_LIMIT = 0.3f;
    const float MAX_LIMIT = 0.8f;
    const unsigned N = 3;
    const unsigned ENTRIES_PER_PAGE = 2;
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