#include "database.h"
#include <iostream>

int main(int argc, char* args[])
{
    Database db;

    while(true)
    {
        char c[2];
        std::cin.getline(c, 2);

        if(c[0] == 'i')
        {
            char key[21];
            char value[51];
            std::cin.getline(key, 21);
            std::cin.getline(value, 51);
            db.InsertEntry(key, value);
        }
        else if(c[0] == 'c')
        {
            char searchToken[21];
            std::cin.getline(searchToken, 21);

            std::vector<std::string> values;
            if(db.GetEntry(searchToken, values) != 0)
            {
                for(unsigned i = 0; i < values.size(); i++)
                {
                    std::cout << searchToken << " " << values[i] << std::endl;
                }
            }
        }
        else if(c[0] == 'r')
        {
            char deleteToken[21];
            std::cin.getline(deleteToken, 21);

            db.DeleteEntry(deleteToken);
        }
        else break;
    }

    return 0;
}