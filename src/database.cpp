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
{ }

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

    // Update index header
    ih.numberOfEntries++;
    
    UpdateIndexHeader(hash, ih);
    UpdateMainHeader();

    // Test if max limit has been exceeded
    float load = CalcLoad();
    if(load > MAX_LIMIT)
    {
        SplitPage(m_next);
    }

    return 0;
}

int Database::GetEntry(const char key[21], std::vector<std::string>& outValues)
{
    bool anyEntryFound = false;
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
                outValues.push_back(e.value);
                anyEntryFound = true;
                // strcpy(outValue, e.value);
            }
            entryNumber++;
            // All entries have been verified
            if(entryNumber == ih.numberOfEntries) break;
        }

        index++;
        if(index % ENTRIES_PER_PAGE == 0)
        {
            dataFile = GetExtensionFile(index / ENTRIES_PER_PAGE);
            dataFile->seekg(hash * INDEX_SIZE);
        }
    }

    if(anyEntryFound) return 1;
    return 0;
}

int Database::DeleteEntry(const char key[21])
{
    std::fstream* dataFile = &m_mainDataFile;
    unsigned hash = CalcHash(key);

    IndexHeader ih;
    dataFile->seekg(CalcIndexOffset(hash));
    dataFile->read((char *)&ih, INDEX_HEADER_SIZE);

     if(ih.numberOfEntries == 0) return 0;

    unsigned numberOfEntries = ih.numberOfEntries;
    unsigned index = 0;
    for(unsigned i = 0; i < numberOfEntries;)
    {
        Entry e = { 0, 0 };
        dataFile->read((char *)&e, ENTRY_SIZE);
        
        if(!IsEntryEmpty(&e))
        {
            if(strcmp(key, e.key) == 0)
            {
                // Move file pointer to the beggining of entry
                dataFile->seekp((unsigned)dataFile->tellp() - ENTRY_SIZE);
                dataFile->write("\0", 1);
                // Move file pointer to the end of entry
                dataFile->seekp((unsigned)dataFile->tellp() + ENTRY_SIZE - 1);

                // Update main and index headers
                ih.numberOfEntries--;
                m_numberOfEntries--;
            }

            // Entry found
            i++;
        }

        index++;

        if(index % ENTRIES_PER_PAGE == 0 && i < numberOfEntries)
        {
            dataFile = GetExtensionFile(index / ENTRIES_PER_PAGE);
            dataFile->seekp(CalcIndexOffset(hash, false));
        }
    }

    // Saves main and index headers to file
    UpdateIndexHeader(hash, ih);
    UpdateMainHeader();

    if(m_numberOfIndices > N) 
    {
        float load = CalcLoad();
        if(load < MIN_LIMIT)
        {
            while(load < MIN_LIMIT && m_numberOfIndices > N)
            {
                MergePage(m_numberOfIndices - 1);
                load = CalcLoad();
            }
        }
    }
    // Checks if at least one entry was deleted
    if(ih.numberOfEntries < numberOfEntries) return 1;
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

float Database::CalcLoad()
{
    return m_numberOfEntries / ((float)m_numberOfPages * ENTRIES_PER_PAGE);
}

void Database::SplitPage(unsigned page)
{
    m_numberOfPages++;
    m_numberOfIndices++;

    // Advances next pointer
    m_next = (m_next + 1) % (N * (TwoToThePower(m_level)));
    if(m_next == 0) m_level++;

    // Fill the next page with 0's
    m_mainDataFile.seekp((m_numberOfIndices + 1) * MAIN_INDEX_SIZE + HEADER_SIZE - 1);
    m_mainDataFile.write("", 1);

    // Fill all the extensions at the new page index with 0's
    for(unsigned i = 1; i <= m_currentMaxExtension; i++)
    {
        std::fstream* dataFile = GetExtensionFile(i);
        dataFile->seekg((m_numberOfIndices + 1) * ENTRIES_PER_PAGE * ENTRY_SIZE - 1);
        dataFile->write("\0", 1);
    }

    DistributeEntries(page);
    UpdateMainHeader();
}

void Database::MergePage(unsigned page)
{
    if(m_level == 0 && m_next == 0) return;

    IndexHeader ih;
    m_mainDataFile.seekg(CalcIndexOffset(page, true));
    m_mainDataFile.read((char *)&ih, INDEX_HEADER_SIZE);

    
    // Update next
    if(m_next != 0) m_next--;
    else
    {
        // Find next next pointer position
        m_level--;
        m_next = N * TwoToThePower(m_level) - 1;

    }
    DistributeEntries(page);

    // Remove from the total number of pages the main
    // index page and all index pages
    m_numberOfPages -= ih.numberOfExtensions + 1;

    // Update number of indices
    m_numberOfIndices--;

    ih.numberOfEntries = 0;
    ih.numberOfExtensions = 0;

    UpdateIndexHeader(page, ih);
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

    unsigned index = 0;
    unsigned originalIndexNumberOfEntries = originalIndexHeader.numberOfEntries;
    for(unsigned i = 0; i < originalIndexNumberOfEntries;)
    {
        Entry e = { 0, 0 };
        currentDataFile->read((char *)&e, ENTRY_SIZE);

        if(!IsEntryEmpty(&e))
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
                // Attention: InsertEntry will reposition the main data
                // file pointers.
                InsertEntry(e.key, e.value);

                // Get pointer again, because it might no longer
                // be in memory
                if(index / ENTRIES_PER_PAGE != 0)
                {
                    currentDataFile = GetExtensionFile(index / ENTRIES_PER_PAGE);
                }
            }
        }

        index++;
        
        if(index % ENTRIES_PER_PAGE == 0 && index < originalIndexNumberOfEntries)
        {
            currentDataFile = GetExtensionFile(index / ENTRIES_PER_PAGE);
            currentDataFile->seekg(CalcIndexOffset(page, false));
        }
        else
        {
            currentDataFile->seekg(
                CalcIndexOffset(page, currentDataFile == &m_mainDataFile) + 
                (index % ENTRIES_PER_PAGE) * ENTRY_SIZE + 
                (currentDataFile == &m_mainDataFile ? INDEX_HEADER_SIZE : 0));  
        }
        
    }

    // Write header to file
    UpdateIndexHeader(page, originalIndexHeader);
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
        ENTRY_SIZE * ENTRIES_PER_PAGE * m_numberOfIndices + INDEX_SIZE - 1);
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