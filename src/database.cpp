#include "database.h"
#include <cstring>

Database::Database()
{
    // Checks if header file exists.
    // If not, create it.
    if(!OpenMainDataFile())
    {
        CreateMainDataFile();
    }

    // Read header data
    Header header = { 0, 0, 0, 0 };
    m_mainDataFile.read((char*)&header, sizeof(Header));
    m_level = header.level;
    m_next = header.next;
}

Database::~Database()
{
    m_mainDataFile.close();
}

bool Database::OpenMainDataFile()
{
    m_mainDataFile.open(MAIN_DATA_FILE_NAME, 
        std::fstream::in    |
        std::fstream::out   | 
        std::fstream::binary);
    
    return m_mainDataFile.is_open();
}

void Database::CreateMainDataFile()
{
    m_mainDataFile.close();
    m_mainDataFile.open(MAIN_DATA_FILE_NAME, 
    std::fstream::out   | 
    std::fstream::binary);

    m_level = 0;
    m_next = 0;
    Header h = { N, m_level, N, m_next };
    m_mainDataFile.write((const char*)&h, sizeof(Header));

    m_mainDataFile.seekp(N * ENTRIES_PER_PAGE * HEADER_SIZE - 1);
    m_mainDataFile.write("", 1);
    m_mainDataFile.close();

    OpenMainDataFile();
}