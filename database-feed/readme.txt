Estrutura de Dados e Algoritmos II
Flávio Assis
Alunos:
Alisson Souza
Gabriel Lacerda
Gustavo Passos
Kaio Carvalho

Para compilar, a pasta database-feed deve estar dentro da raiz da pasta
do trabalho enviado anteriormente, pois o arquivo busca pelas seguintes
dependências:
    ../src/database.h
    ../src/database.cpp

O arquivo pode, então, ser compilado através do comando 'make'. O execu-
tável estará dentro da pasta bin.
*************************************************************************

Após rodar do executável com um arquivo de entrada, os arquivos gerados
para o banco de dados (dataxx.dat, onde xx são números) estarão dentro 
da pasta do bin. Basta então copiar esses arquivos para o diretório do 
executável principal. 
