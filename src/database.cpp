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
    m_numberOfPages = header.numberOfPages;
    m_numberOfEntries = header.numberOfEntires;
    m_next = header.next;
}

Database::~Database()
{
    // Stores header data to file
    Header h = { N, m_level, m_numberOfPages, m_numberOfEntries, m_next };
    m_mainDataFile.seekp(std::ios::beg);
    m_mainDataFile.write((const char*)&h, sizeof(Header));

    m_mainDataFile.close();
}

int Database::InsertEntry(const char key[21], const char value[51])
{
    unsigned hash = CalcHash(key);
    unsigned offset = CalcIndexOffset(hash);

    m_mainDataFile.seekp(offset, std::ios::beg);
    IndexHeader ih;
    m_mainDataFile.read((char*)&ih, INDEX_HEADER_SIZE);
    std::streampos firstEmptyPos = m_mainDataFile.tellp();

    // If there are entries, find the first empty slot.
    // Otherwise, just write at the first slot.
    if(ih.numberOfEntries != 0)
    {
        firstEmptyPos = FindFirstEmptySlot();
    }

    WriteEntryAtPosition(key, value, firstEmptyPos);
    
    // Update index header
    ih.numberOfEntries++;
    m_mainDataFile.seekp(offset);
    m_mainDataFile.write((const char*)&ih, INDEX_HEADER_SIZE);

    // Update database header (only in memory)
    // The final value is stored only when the program is closed
    m_numberOfEntries++;

    return 0;
}

int Database::GetEntry(const char key[21], char outValue[51])
{
    unsigned index_offset = CalcOffsetFromKey(key);

    unsigned offset = index_offset + INDEX_HEADER_SIZE;
    m_mainDataFile.seekp(offset, std::ios::beg);
    while(offset < index_offset + INDEX_SIZE)
    {
        Entry e = { 0, 0, 0, 0};
        m_mainDataFile.read((char *)&e, ENTRY_SIZE);
        offset += ENTRY_SIZE;

        if(strcmp(e.key, key) == 0)
        {
            strcpy(outValue, e.value);
            return 1;
        }
    }

    return 0;
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
    Header h = { N, m_level, N, 0, m_next };
    m_mainDataFile.write((const char*)&h, sizeof(Header));

    // Fill file with empty data
    m_mainDataFile.seekp(
        N * INDEX_SIZE + HEADER_SIZE - 1);
    m_mainDataFile.write("", 1);
    m_mainDataFile.close();

    OpenMainDataFile();
}

unsigned Database::CalcHash(const char key[21])
{
    return (int)key[0] % (N * PowerOfTwo(m_level));
}

unsigned Database::PowerOfTwo(int exponent)
{
    if(exponent == 0) return 1;
    return 2 << exponent;
}

unsigned Database::CalcIndexOffset(int index)
{
    return HEADER_SIZE + index * INDEX_SIZE;
}

unsigned Database::CalcOffsetFromKey(const char key[21])
{
    return CalcIndexOffset(CalcHash(key));
}

std::streampos Database::FindFirstEmptySlot()
{
    unsigned startOffset = m_mainDataFile.tellg();
    while(m_mainDataFile.tellp() < startOffset + INDEX_SIZE)
    {
        char check = '\0';
        m_mainDataFile.read(&check, 1);
        if(check == '\0') return ((int)m_mainDataFile.tellp() - 1);
        m_mainDataFile.seekp((int)m_mainDataFile.tellp() + ENTRY_SIZE - 1);
    }

    return -1;
}

void Database::WriteEntryAtPosition(
        const char key[21], 
        const char value[51], 
        const std::streampos& pos)
{
    m_mainDataFile.seekp(pos);
    m_mainDataFile.write((const char*)key, 21);
    m_mainDataFile.write((const char*)value, 51);
}
