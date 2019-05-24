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
    Header header = { 0, 0, 0, 0 , 0, 0 };
    m_mainDataFile.read((char*)&header, HEADER_SIZE);
    m_level = header.level;
    m_numberOfEntries = header.numberOfEntries;
    m_numberOfPages = header.numberOfPages;
    m_numberOfIndices = header.numberOfIndices;
    m_next = header.next;
    m_currentMaxExtension = header.currentMaxExtension;
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
    unsigned firstEmptyPos = 0;

    // If there are entries, find the first empty slot.
    // Otherwise, just write at the first slot.
    if(ih.numberOfEntries != 0)
    {
        // Check if entry is full
        if(ih.numberOfEntries == (ih.numberOfExtensions + 1) * ENTRIES_PER_PAGE)
        {
            ExtendIndex(hash);
            // Update index header value
            ih.numberOfExtensions++;
        }

        firstEmptyPos = FindFirstEmptySlot(hash);
    }

    WriteEntry(hash, firstEmptyPos, key, value);
    
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
    
    UpdateIndexHeader(hash, ih);
    UpdateMainHeader();

    return 0;
}

int Database::GetEntry(const char key[21], char outValue[51])
{
    std::fstream* dataFile = &m_mainDataFile;
    unsigned index_offset = CalcIndexOffsetFromKey(key);
    unsigned hash = CalcHash(key);

    dataFile->seekg(index_offset);
    IndexHeader ih;
    dataFile->read((char *)&ih, INDEX_HEADER_SIZE);

    // There are no entries
    if(ih.numberOfEntries == 0) return 0;

    unsigned index = 0;
    unsigned entryNumber = 0;
    while(true)
    {
        Entry e = { 0, 0 };
        dataFile->read((char *)&e, ENTRY_SIZE);

        if(!IsEntryEmpty(&e))
        {
            if(strcmp(e.key, key) == 0)
            {
                strcpy(outValue, e.value);
                return 1;
            }
            entryNumber++;
            // All entries have been verified
            if(entryNumber == ih.numberOfEntries) return 0;
        }

        index++;
        if(index % ENTRIES_PER_PAGE == 0)
        {
            dataFile = GetExtensionFile(index / ENTRIES_PER_PAGE);
            dataFile->seekg(hash * INDEX_SIZE);
        }
    }

    return 0;
}

bool Database::OpenMainDataFile()
{
    m_mainDataFile.open(DATA_FILE_NAME_PREFIX + DATA_FILE_NAME_EXTENSION,
        std::fstream::in    |
        std::fstream::out   | 
        std::fstream::binary);
    
    return m_mainDataFile.is_open();
}

void Database::CreateMainDataFile()
{
    m_mainDataFile.close();
    m_mainDataFile.open(DATA_FILE_NAME_PREFIX + DATA_FILE_NAME_EXTENSION, 
    std::fstream::out   | 
    std::fstream::binary);

    m_level = 0;
    m_next = 0;
    Header h = { N, m_level, N, 0, N, m_next, 0 };
    m_mainDataFile.write((const char*)&h, sizeof(Header));

    // Fill file with empty data
    m_mainDataFile.seekp(
        N * MAIN_INDEX_SIZE + HEADER_SIZE - 1);
    m_mainDataFile.write("", 1);
    m_mainDataFile.close();

    OpenMainDataFile();
}

void Database::UpdateMainHeader()
{
    // Stores header data to file
    Header h = { 
        N, 
        m_level, 
        m_numberOfPages, 
        m_numberOfEntries, 
        m_numberOfIndices, 
        m_next,
        m_currentMaxExtension
    };
    m_mainDataFile.seekp(std::ios::beg);
    m_mainDataFile.write((const char*)&h, sizeof(Header));
}

void Database::UpdateIndexHeader(unsigned index, const IndexHeader& ih)
{
    m_mainDataFile.seekp(CalcIndexOffset(index));
    m_mainDataFile.write((const char*)&ih, INDEX_HEADER_SIZE);
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
    return (isMainFile ? HEADER_SIZE : 0) + index * ((isMainFile ? INDEX_HEADER_SIZE : 0) + INDEX_SIZE);
}

unsigned Database::CalcIndexOffsetFromKey(const char key[21])
{
    return CalcIndexOffset(CalcHash(key));
}

unsigned Database::FindFirstEmptySlot(unsigned index)
{
    std::fstream* dataFile = &m_mainDataFile;
    dataFile->seekg(CalcIndexOffset(index, true) + INDEX_HEADER_SIZE);

    unsigned currentIndex = 0;
    while(true)
    {
        Entry e = { 0, 0 };
        dataFile->read((char *)&e, ENTRY_SIZE);

        if(IsEntryEmpty(&e)) return currentIndex;

        currentIndex++;
        if(currentIndex % ENTRIES_PER_PAGE == 0)
        {
            dataFile = GetExtensionFile(currentIndex / ENTRIES_PER_PAGE);
            dataFile->seekg(CalcIndexOffset(index, false));
        }
    }

    if(!dataFile->good()) std::cout << "Stream is bad!" << std::endl;
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

void Database::WriteEntry(
    int index,
    int position,
    const char key[21],
    const char value[51])
{
    std::fstream* file = &m_mainDataFile;

    unsigned offset = 0;
    int extensionNumber = position / ENTRIES_PER_PAGE;
    if(extensionNumber == 0)
    {
        offset = CalcIndexOffset(index, true) + INDEX_HEADER_SIZE;
    }
    else
    {
        file = GetExtensionFile(extensionNumber);
        offset = CalcIndexOffset(index, false);
    }

    file->seekp(offset + (position % ENTRIES_PER_PAGE) * ENTRY_SIZE);
    file->write(key, 21);
    file->write(value, 51);
}

bool Database::IsEntryEmpty(const Entry* entry)
{
    return entry->key[0] == '\0';
}

void Database::SplitPage(unsigned page)
{
    m_numberOfPages++;
    m_numberOfIndices++;
    // Advances next pointer
    m_next = (m_next + 1) % (N * (m_level + 1));
    if(m_next == 0) m_level++;

    // Fill the next page with 0's
    m_mainDataFile.seekp((m_numberOfPages + 1) * MAIN_INDEX_SIZE + HEADER_SIZE - 2);
    m_mainDataFile.write("", 1);

    DistributeEntries(page);
    UpdateMainHeader();
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

void Database::ExtendIndex(unsigned index)
{
    m_mainDataFile.seekg(CalcIndexOffset(index));
    IndexHeader ih;    
    m_mainDataFile.read((char*)&ih, INDEX_HEADER_SIZE);

    if(ih.numberOfExtensions + 1 > m_currentMaxExtension)
    {
        CreateExtensionFile(ih.numberOfExtensions + 1);
        m_currentMaxExtension++;
    }

    m_numberOfPages++;
    UpdateMainHeader();
}

void Database::CreateExtensionFile(unsigned extensionNumber)
{
    if(m_indexExtensionsDataFiles.size() == MAX_SIMULTANEOUS_EXTENSIONS_OPEN)
    {
        m_indexExtensionsDataFiles.front().second.close();
        m_indexExtensionsDataFiles.erase(m_indexExtensionsDataFiles.begin());
    }

    m_indexExtensionsDataFiles.push_back(
        std::make_pair(extensionNumber, std::fstream()));

    m_indexExtensionsDataFiles.back().second.open(
        DATA_FILE_NAME_PREFIX           +
        std::to_string(extensionNumber) +
        DATA_FILE_NAME_EXTENSION,
        std::fstream::out   |
        std::fstream::binary
    );

    m_indexExtensionsDataFiles.back().second.seekp(
        ENTRY_SIZE * m_numberOfIndices + INDEX_SIZE - 1);
    m_indexExtensionsDataFiles.back().second.write("\0", 1);

    m_indexExtensionsDataFiles.back().second.close();

    m_indexExtensionsDataFiles.back().second.open(
        DATA_FILE_NAME_PREFIX           +
        std::to_string(extensionNumber) +
        DATA_FILE_NAME_EXTENSION,
        std::fstream::in    |
        std::fstream::out   |
        std::fstream::binary
    );

    UpdateMainHeader(); 
}

std::fstream* Database::GetExtensionFile(unsigned extensionNumber)
{
    for(auto ep = m_indexExtensionsDataFiles.begin();
        ep != m_indexExtensionsDataFiles.end();
        ep++)
    {
        if(ep->first == extensionNumber) return &ep->second;
    }

    if(m_indexExtensionsDataFiles.size() == MAX_SIMULTANEOUS_EXTENSIONS_OPEN)
    {
        m_indexExtensionsDataFiles.front().second.close();
        m_indexExtensionsDataFiles.erase(m_indexExtensionsDataFiles.begin());
    }

    m_indexExtensionsDataFiles.push_back(
        std::make_pair(extensionNumber, std::fstream())
    );

    m_indexExtensionsDataFiles.back().second.open(
        DATA_FILE_NAME_PREFIX           +
        std::to_string(extensionNumber) +
        DATA_FILE_NAME_EXTENSION,
        std::fstream::in    |
        std::fstream::out   |
        std::fstream::binary
    );

    return &(m_indexExtensionsDataFiles.back().second);
}