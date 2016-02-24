# olaper
OLAP XMLA interface to your database





mysql> create database foodmart  default character set utf8 default collate utf8_general_ci;
Query OK, 1 row affected (0,01 sec)

mysql> create user 'foodmart'@'localhost' identified by 'foodmart';
Query OK, 0 rows affected (0,11 sec)

mysql> grant ALL privileges on foodmart.* to 'foodmart'@'localhost' identified by 'foodmart' with grant option;
Query OK, 0 rows affected (0,00 sec)

checkout the project https://github.com/OSBI/foodmart-data


MBP-Aleksej:foodmart-data-master studnev$ cd data/
MBP-Aleksej:data studnev$ unzip DataScript.zip 
Archive:  DataScript.zip
  inflating: FoodMartCreateData.sql  
MBP-Aleksej:data studnev$ ls
DataScript.zip		FoodMartCreateData.sql
MBP-Aleksej:data studnev$ cd ..
MBP-Aleksej:foodmart-data-master studnev$ sh FoodMartLoader.sh --db mysql