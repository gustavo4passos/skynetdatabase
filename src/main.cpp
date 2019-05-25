#include "database.h"
#include <iostream>

int main(int argc, char* args[])
{
    Database db;

    while(true)
    {
        std::cout << "Which action >>";
        char c[2];
        std::cin.getline(c, 2);

        if(c[0] == 'i')
        {
            char key[21];
            char value[51];
            std::cout << "Type a key >> ";
            std::cin.getline(key, 21);
            std::cout << "Type a value >>";
            std::cin.getline(value, 51);
            db.InsertEntry(key, value);
        }
        else if(c[0] == 'c')
        {
            std::cout << "Which key to search for >> ";
            char searchToken[21];
            char valueFound[51];
            std::cin.getline(searchToken, 21);

            if(db.GetEntry(searchToken, valueFound) == 0)
            {
                std::cout << "No such key exists." << std::endl;
            }
            else
            {
                std::cout << "Value: " << valueFound << std::endl;
            }
        }
        else if(c[0] == 'r')
        {
            std::cout << "Which key to remove >> ";
            char deleteToken[21];
            std::cin.getline(deleteToken, 21);

            if(db.DeleteEntry(deleteToken) == 0)
            {
                std::cout << "Unable to delete key: no such key exists." << std::endl;
            }
        }
        else break;
    }

    return 0;
}