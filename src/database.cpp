#include "database.h"
#include <iostream>
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
    m_mainDataFile.close();
}

int Database::InsertEntry(const char key[21], const char value[51])
{
    unsigned hash = CalcHash(key);
    unsigned indexOffset = CalcIndexOffset(hash);

    m_mainDataFile.seekp(indexOffset, std::ios::beg);
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
    
    // Update database header (only in memory)
    // The final value is stored only when the program is closed
    m_numberOfEntries++;

    // Test if max limit has been exceeded
    float load = m_numberOfEntries / ((float)m_numberOfPages * ENTRIES_PER_PAGE);
    if(load > MAX_LIMIT)
    {
        SplitPage(m_next);
    }

    // Update index header
    ih.numberOfEntries++;
    m_mainDataFile.seekp(indexOffset);
    m_mainDataFile.write((const char*)&ih, INDEX_HEADER_SIZE);

    UpdateMainHeader();

    return 0;
}

int Database::GetEntry(const char key[21], char outValue[51])
{
    unsigned index_offset = CalcIndexOffsetFromKey(key);

    unsigned offset = index_offset + INDEX_HEADER_SIZE;
    m_mainDataFile.seekg(offset, std::ios::beg);
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

void Database::UpdateMainHeader()
{
    // Stores header data to file
    Header h = { N, m_level, m_numberOfPages, m_numberOfEntries, m_next };
    m_mainDataFile.seekp(std::ios::beg);
    m_mainDataFile.write((const char*)&h, sizeof(Header));
}

unsigned Database::CalcHash(const char key[21])
{
    unsigned hash = (unsigned)key[0] % (N * TwoToThePower(m_level));
    if(hash < m_next) return (unsigned)key[0] % (N * TwoToThePower(m_level + 1));
    return hash;
}

unsigned Database::TwoToThePower(int exponent)
{
    if(exponent == 0) return 1;
    return 2 << (exponent - 1);
}

unsigned Database::CalcIndexOffset(int index, bool isMainFile)
{
    return (isMainFile ? HEADER_SIZE : 0) + index * INDEX_SIZE;
}

unsigned Database::CalcIndexOffsetFromKey(const char key[21])
{
    return CalcIndexOffset(CalcHash(key));
}

std::streampos Database::FindFirstEmptySlot()
{
    unsigned startOffset = m_mainDataFile.tellp();
    while(m_mainDataFile.tellp() < startOffset + INDEX_SIZE)
    {
        char check = '\0';
        m_mainDataFile.read(&check, 1);
        if(check == '\0') return ((int)m_mainDataFile.tellp() - 1);
        m_mainDataFile.seekp((int)m_mainDataFile.tellp() + ENTRY_SIZE - 1);
    }


    if(!m_mainDataFile.good()) std::cout << "Stream is bad!" << std::endl;
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

void Database::SplitPage(unsigned page)
{
    m_numberOfPages++;
    // Advances next pointer
    m_next = (m_next + 1) % (N * (m_level + 1));
    if(m_next == 0) m_level++;

    // Fill the next page with 0's
    m_mainDataFile.seekp((m_numberOfPages + 1) * INDEX_SIZE + HEADER_SIZE - 2);
    m_mainDataFile.write("", 1);

    DistributeEntries(page);
}

void Database::DistributeEntries(unsigned page)
{
    IndexHeader originalIndexHeader;
    std::fstream* currentDataFile = &m_mainDataFile;

    // Go to first entry in the index
    m_mainDataFile.seekp(CalcIndexOffset(page));
    m_mainDataFile.read((char *)&originalIndexHeader, INDEX_HEADER_SIZE);
    if(originalIndexHeader.numberOfEntries == 0) return;

    for(unsigned i = 0; i < originalIndexHeader.numberOfEntries;)
    {
        Entry e = { 0, 0 };
        currentDataFile->read((char *)&e, ENTRY_SIZE);

        // Check if entry is empty
        if(e.key[0] != '\0')
        {
            // Advance entry index
            i++;

            unsigned hash = CalcHash(e.key);
            if(hash != page)
            {
                // Returns pointer to beggining of entry
                currentDataFile->seekp(
                    (unsigned)currentDataFile->tellp() - ENTRY_SIZE);

                // Delete entry from current index,
                // i.e. sets the first byte of
                // its key to \0
                currentDataFile->write("\0", 1);
                
                // Update total number of entries before inserting
                // entry into split page
                m_numberOfEntries--;

                // Update index header
                originalIndexHeader.numberOfEntries--;

                // Puts file pointer back at the end of current entry
                currentDataFile->seekp(
                    (unsigned)currentDataFile->tellp() + ENTRY_SIZE - 1);

                // Insert into the new page
                InsertEntry(e.key, e.value);

                continue;
            }
        }
    }
    // Write header to file
    m_mainDataFile.seekp(CalcIndexOffset(page));
    m_mainDataFile.write((const char*)&originalIndexHeader, INDEX_HEADER_SIZE);
}
