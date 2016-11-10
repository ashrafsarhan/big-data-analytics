/*
 #######  #####   #####     #    ####### #          ######  ######     #                           #      
 #    #  #     # #     #   # #   #       #    #     #     # #     #    #         ##   #####       ##      
     #         #       #  #   #  #       #    #     #     # #     #    #        #  #  #    #     # #      
    #     #####   #####  #     # ######  #    #     #     # ######     #       #    # #####        #      
   #           # #       #######       # #######    #     # #     #    #       ###### #    # ###   #      
   #     #     # #       #     # #     #      #     #     # #     #    #       #    # #    # ###   #      
   #      #####  ####### #     #  #####       #     ######  ######     ####### #    # #####  ### #####    
*/
                                                                                                         
--1
SELECT * FROM jbemployee;
/*
+------+--------------------+--------+---------+-----------+-----------+
| id   | name               | salary | manager | birthyear | startyear |
+------+--------------------+--------+---------+-----------+-----------+
|   10 | Ross, Stanley      |  15908 |     199 |      1927 |      1945 |
|   11 | Ross, Stuart       |  12067 |    NULL |      1931 |      1932 |
|   13 | Edwards, Peter     |   9000 |     199 |      1928 |      1958 |
|   26 | Thompson, Bob      |  13000 |     199 |      1930 |      1970 |
|   32 | Smythe, Carol      |   9050 |     199 |      1929 |      1967 |
|   33 | Hayes, Evelyn      |  10100 |     199 |      1931 |      1963 |
|   35 | Evans, Michael     |   5000 |      32 |      1952 |      1974 |
|   37 | Raveen, Lemont     |  11985 |      26 |      1950 |      1974 |
|   55 | James, Mary        |  12000 |     199 |      1920 |      1969 |
|   98 | Williams, Judy     |   9000 |     199 |      1935 |      1969 |
|  129 | Thomas, Tom        |  10000 |     199 |      1941 |      1962 |
|  157 | Jones, Tim         |  12000 |     199 |      1940 |      1960 |
|  199 | Bullock, J.D.      |  27000 |    NULL |      1920 |      1920 |
|  215 | Collins, Joanne    |   7000 |      10 |      1950 |      1971 |
|  430 | Brunet, Paul C.    |  17674 |     129 |      1938 |      1959 |
|  843 | Schmidt, Herman    |  11204 |      26 |      1936 |      1956 |
|  994 | Iwano, Masahiro    |  15641 |     129 |      1944 |      1970 |
| 1110 | Smith, Paul        |   6000 |      33 |      1952 |      1973 |
| 1330 | Onstad, Richard    |   8779 |      13 |      1952 |      1971 |
| 1523 | Zugnoni, Arthur A. |  19868 |     129 |      1928 |      1949 |
| 1639 | Choy, Wanda        |  11160 |      55 |      1947 |      1970 |
| 2398 | Wallace, Maggie J. |   7880 |      26 |      1940 |      1959 |
| 4901 | Bailey, Chas M.    |   8377 |      32 |      1956 |      1975 |
| 5119 | Bono, Sonny        |  13621 |      55 |      1939 |      1963 |
| 5219 | Schwarz, Jason B.  |  13374 |      33 |      1944 |      1959 |
+------+--------------------+--------+---------+-----------+-----------+
25 rows in set (0,00 sec)
*/

#2
mysql> SELECT jbdept.name AS dept_name FROM jbdept ORDER BY dept_name;
+------------------+
| dept_name        |
+------------------+
| Bargain          |
| Book             |
| Candy            |
| Children's       |
| Children's       |
| Furniture        |
| Giftwrap         |
| Jewelry          |
| Junior Miss      |
| Junior's         |
| Linens           |
| Major Appliances |
| Men's            |
| Sportswear       |
| Stationary       |
| Toys             |
| Women's          |
| Women's          |
| Women's          |
+------------------+
19 rows in set (0,00 sec)

#3
mysql> SELECT * FROM jbparts WHERE jbparts.qoh=0;
+----+-------------------+-------+--------+------+
| id | name              | color | weight | qoh  |
+----+-------------------+-------+--------+------+
| 11 | card reader       | gray  |    327 |    0 |
| 12 | card punch        | gray  |    427 |    0 |
| 13 | paper tape reader | black |    107 |    0 |
| 14 | paper tape punch  | black |    147 |    0 |
+----+-------------------+-------+--------+------+
4 rows in set (0,00 sec)

#4
mysql> SELECT * FROM jbemployee WHERE jbemployee.salary BETWEEN 9000 AND 10000;
+-----+----------------+--------+---------+-----------+-----------+
| id  | name           | salary | manager | birthyear | startyear |
+-----+----------------+--------+---------+-----------+-----------+
|  13 | Edwards, Peter |   9000 |     199 |      1928 |      1958 |
|  32 | Smythe, Carol  |   9050 |     199 |      1929 |      1967 |
|  98 | Williams, Judy |   9000 |     199 |      1935 |      1969 |
| 129 | Thomas, Tom    |  10000 |     199 |      1941 |      1962 |
+-----+----------------+--------+---------+-----------+-----------+
4 rows in set (0,00 sec)


#5
mysql> SELECT emp.id, emp.name, (emp.startyear-emp.birthyear) AS age FROM jbemployee AS emp;
+------+--------------------+------+
| id   | name               | age  |
+------+--------------------+------+
|   10 | Ross, Stanley      |   18 |
|   11 | Ross, Stuart       |    1 |
|   13 | Edwards, Peter     |   30 |
|   26 | Thompson, Bob      |   40 |
|   32 | Smythe, Carol      |   38 |
|   33 | Hayes, Evelyn      |   32 |
|   35 | Evans, Michael     |   22 |
|   37 | Raveen, Lemont     |   24 |
|   55 | James, Mary        |   49 |
|   98 | Williams, Judy     |   34 |
|  129 | Thomas, Tom        |   21 |
|  157 | Jones, Tim         |   20 |
|  199 | Bullock, J.D.      |    0 |
|  215 | Collins, Joanne    |   21 |
|  430 | Brunet, Paul C.    |   21 |
|  843 | Schmidt, Herman    |   20 |
|  994 | Iwano, Masahiro    |   26 |
| 1110 | Smith, Paul        |   21 |
| 1330 | Onstad, Richard    |   19 |
| 1523 | Zugnoni, Arthur A. |   21 |
| 1639 | Choy, Wanda        |   23 |
| 2398 | Wallace, Maggie J. |   19 |
| 4901 | Bailey, Chas M.    |   19 |
| 5119 | Bono, Sonny        |   24 |
| 5219 | Schwarz, Jason B.  |   15 |
+------+--------------------+------+
25 rows in set (0,00 sec)


#6
mysql> SELECT * FROM jbemployee AS emp WHERE emp.name LIKE '%son';
Empty set (0,00 sec)


#7

mysql> SELECT * FROM jbitem AS itm WHERE itm.supplier = (SELECT sup.id FROM jbsupplier as sup WHERE sup.name LIKE 'Fisher-Price');
+-----+-----------------+------+-------+------+----------+
| id  | name            | dept | price | qoh  | supplier |
+-----+-----------------+------+-------+------+----------+
|  43 | Maze            |   49 |   325 |  200 |       89 |
| 107 | The 'Feel' Book |   35 |   225 |  225 |       89 |
| 119 | Squeeze Ball    |   49 |   250 |  400 |       89 |
+-----+-----------------+------+-------+------+----------+
3 rows in set (0,00 sec)


#8

mysql> SELECT itm.id, itm.name, itm.dept, itm.price, itm.qoh, itm.supplier FROM jbitem AS itm, jbsupplier AS sup WHERE itm.supplier = sup.id AND sup.name LIKE 'Fisher-Price';
+-----+-----------------+------+-------+------+----------+
| id  | name            | dept | price | qoh  | supplier |
+-----+-----------------+------+-------+------+----------+
|  43 | Maze            |   49 |   325 |  200 |       89 |
| 107 | The 'Feel' Book |   35 |   225 |  225 |       89 |
| 119 | Squeeze Ball    |   49 |   250 |  400 |       89 |
+-----+-----------------+------+-------+------+----------+
3 rows in set (0,16 sec)



#9

mysql> SELECT * FROM jbcity AS cty WHERE cty.id IN (SELECT sup.city FROM jbsupplier AS sup);
+-----+----------------+-------+
| id  | name           | state |
+-----+----------------+-------+
|  10 | Amherst        | Mass  |
|  21 | Boston         | Mass  |
| 100 | New York       | NY    |
| 106 | White Plains   | Neb   |
| 118 | Hickville      | Okla  |
| 303 | Atlanta        | Ga    |
| 537 | Madison        | Wisc  |
| 609 | Paxton         | Ill   |
| 752 | Dallas         | Tex   |
| 802 | Denver         | Colo  |
| 841 | Salt Lake City | Utah  |
| 900 | Los Angeles    | Calif |
| 921 | San Diego      | Calif |
| 941 | San Francisco  | Calif |
| 981 | Seattle        | Wash  |
+-----+----------------+-------+
15 rows in set (0,00 sec)

#10
mysql> SELECT prts.name, prts.color FROM jbparts AS prts WHERE prts.weight > (SELECT prts.weight FROM jbparts AS prts WHERE prts.name = 'card reader');
+--------------+--------+
| name         | color  |
+--------------+--------+
| disk drive   | black  |
| tape drive   | black  |
| line printer | yellow |
| card punch   | gray   |
+--------------+--------+
4 rows in set (0,00 sec)


#11
mysql> SELECT prts.name, prts.color FROM jbparts AS prts, jbparts AS prts2 WHERE prts2.name = 'card reader' AND prts.weight > prts2.weight;
+--------------+--------+
| name         | color  |
+--------------+--------+
| disk drive   | black  |
| tape drive   | black  |
| line printer | yellow |
| card punch   | gray   |
+--------------+--------+
4 rows in set (0,00 sec)



#12
mysql> SELECT avg(prts.weight) AS avg_weight FROM jbparts AS prts WHERE prts.color = 'black';
+------------+
| avg_weight |
+------------+
|   347.2500 |
+------------+
1 row in set (0,00 sec)

#13
mysql> SELECT sup.name, SUM(prts.weight*prts.qoh) AS total_weight 
    -> FROM jb.jbparts AS prts, jbsupplier AS sup, jbsupply AS sp, jbcity AS cty 
    -> WHERE prts.id = sp.part AND sup.id = sp.supplier 
    -> AND sup.city = cty.id AND cty.state = 'Mass'
    -> GROUP BY sup.id;
+--------------+--------------+
| name         | total_weight |
+--------------+--------------+
| Fisher-Price |         3170 |
| DEC          |         4470 |
+--------------+--------------+
2 rows in set (0,00 sec)

#14
mysql> CREATE TABLE jbitem_replica
    -> (
    -> `id` INT(11),
    -> `name` VARCHAR(20),
    -> `price` INT(11),
    -> `qoh` INT(10) UNSIGNED,
    -> `dept` INT(11),
    -> `supplier` INT(11),
    -> PRIMARY KEY (`id`),
    -> FOREIGN KEY (`dept`) REFERENCES `jbdept`(`id`),
    -> FOREIGN KEY (`supplier`) REFERENCES `jbsupplier`(`id`)
    -> );
Query OK, 0 rows affected (0,47 sec)

mysql> INSERT INTO jb.jbitem_replica(`id`, `name`, `price`, `qoh`, `dept`, `supplier`)
    ->   SELECT itm.id, itm.name, itm.price, itm.qoh, itm.dept, itm.supplier
    ->   FROM jb.jbitem AS itm 
    ->   WHERE (itm.price) < (SELECT AVG(jb.jbitem.price) FROM jb.jbitem);
Query OK, 14 rows affected (0,14 sec)
Records: 14  Duplicates: 0  Warnings: 0
   
mysql> SELECT * FROM jb.jbitem_replica;
+-----+-----------------+-------+------+------+----------+
| id  | name            | price | qoh  | dept | supplier |
+-----+-----------------+-------+------+------+----------+
|  11 | Wash Cloth      |    75 |  575 |    1 |      213 |
|  19 | Bellbottoms     |   450 |  600 |   43 |       33 |
|  21 | ABC Blocks      |   198 |  405 |    1 |      125 |
|  23 | 1 lb Box        |   215 |  100 |   10 |       42 |
|  25 | 2 lb Box, Mix   |   450 |   75 |   10 |       42 |
|  26 | Earrings        |  1000 |   20 |   14 |      199 |
|  43 | Maze            |   325 |  200 |   49 |       89 |
| 106 | Clock Book      |   198 |  150 |   49 |      125 |
| 107 | The 'Feel' Book |   225 |  225 |   35 |       89 |
| 118 | Towels, Bath    |   250 | 1000 |   26 |      213 |
| 119 | Squeeze Ball    |   250 |  400 |   49 |       89 |
| 120 | Twin Sheet      |   800 |  750 |   26 |      213 |
| 165 | Jean            |   825 |  500 |   65 |       33 |
| 258 | Shirt           |   650 | 1200 |   58 |       33 |
+-----+-----------------+-------+------+------+----------+
14 rows in set (0,00 sec)

#15
mysql> CREATE VIEW jbitem_view AS
    ->      SELECT itm.id, itm.name, itm.price, itm.qoh, itm.dept, itm.supplier
    ->      FROM jb.jbitem AS itm    
    ->      WHERE (itm.price) < (SELECT AVG(jb.jbitem.price) FROM jb.jbitem);
Query OK, 0 rows affected (0,06 sec)
      
mysql> SELECT * FROM jb.jbitem_view;
+-----+-----------------+-------+------+------+----------+
| id  | name            | price | qoh  | dept | supplier |
+-----+-----------------+-------+------+------+----------+
|  11 | Wash Cloth      |    75 |  575 |    1 |      213 |
|  19 | Bellbottoms     |   450 |  600 |   43 |       33 |
|  21 | ABC Blocks      |   198 |  405 |    1 |      125 |
|  23 | 1 lb Box        |   215 |  100 |   10 |       42 |
|  25 | 2 lb Box, Mix   |   450 |   75 |   10 |       42 |
|  26 | Earrings        |  1000 |   20 |   14 |      199 |
|  43 | Maze            |   325 |  200 |   49 |       89 |
| 106 | Clock Book      |   198 |  150 |   49 |      125 |
| 107 | The 'Feel' Book |   225 |  225 |   35 |       89 |
| 118 | Towels, Bath    |   250 | 1000 |   26 |      213 |
| 119 | Squeeze Ball    |   250 |  400 |   49 |       89 |
| 120 | Twin Sheet      |   800 |  750 |   26 |      213 |
| 165 | Jean            |   825 |  500 |   65 |       33 |
| 258 | Shirt           |   650 | 1200 |   58 |       33 |
+-----+-----------------+-------+------+------+----------+
14 rows in set (0,00 sec)

#16
A view is a virtual dynamic table.The difference between a view and a table (static) is that views are definitions built on top of other tables (or views), and do not hold data themselves. If data is changing in the underlying table, the same change is reflected in the view.

#17
mysql> CREATE VIEW jbdebit_view AS
    ->      SELECT dbt.id, dbt.account, SUM(s.quantity*itm.price) AS total_cost
    ->      FROM   jb.jbdebit AS dbt, jb.jbsale AS s, jb.jbitem AS itm
    ->      WHERE  dbt.id = s.debit AND s.item = itm.id
    ->      GROUP BY dbt.id;
Query OK, 0 rows affected (0,06 sec)
   
mysql> SELECT * FROM jb.jbdebit_view;
+--------+----------+------------+
| id     | account  | total_cost |
+--------+----------+------------+
| 100581 | 10000000 |       2050 |
| 100582 | 14356540 |       1000 |
| 100586 | 14096831 |      13446 |
| 100592 | 10000000 |        650 |
| 100593 | 11652133 |        430 |
| 100594 | 12591815 |       3295 |
+--------+----------+------------+
6 rows in set (0,00 sec)

#18
#Using INNER JOIN returns all rows when there is at least one match in both tables.
mysql> CREATE VIEW jbdebit_view_2 AS
    ->      SELECT dbt.id, dbt.account, SUM(s.quantity*itm.price) AS total_cost
    ->      FROM jb.jbdebit AS dbt
    ->      JOIN (jb.jbsale AS s) ON (dbt.id = s.debit)
    ->      JOIN (jb.jbitem AS itm) ON (s.item = itm.id)
    ->      GROUP BY dbt.id;
Query OK, 0 rows affected (0,07 sec)
     
mysql> SELECT * FROM jb.jbdebit_view_2;
+--------+----------+------------+
| id     | account  | total_cost |
+--------+----------+------------+
| 100581 | 10000000 |       2050 |
| 100582 | 14356540 |       1000 |
| 100586 | 14096831 |      13446 |
| 100592 | 10000000 |        650 |
| 100593 | 11652133 |        430 |
| 100594 | 12591815 |       3295 |
+--------+----------+------------+
6 rows in set (0,00 sec)

#19
#Modify the supplier column in jbitem table to be nullable.

ALTER TABLE `jbitem` 
MODIFY COLUMN `supplier` INT(11);

#Drop the current fk_item_supplier key with ON DELETE RESTRICT rule.

ALTER TABLE `jbitem` 
DROP FOREIGN KEY `fk_item_supplier`;

#Drop the current fk_item_supplier key with ON DELETE SET NULL to set rule to #set Null instead of the deleted supplier.   
                                 
ALTER TABLE `jbitem`
ADD CONSTRAINT `fk_item_supplier`
FOREIGN KEY (`supplier`) REFERENCES `jbsupplier` (`id`)
ON DELETE SET NULL;

#Delete the suppliers in Los Angeles

DELETE FROM  jb.jbsupplier
       WHERE jb.jbsupplier.city = (SELECT cty.id 
								    FROM jb.jbcity AS cty 
									WHERE cty.name = 'Los Angeles'); 

#20
mysql> CREATE VIEW jbsale_supply(`supplier`, `item`, `delivered_qty`, `sold_qty`) AS
    -> SELECT sup.name, itm.name, itm.qoh, sale.quantity
    -> FROM jbsupplier AS sup
    -> JOIN (jbitem AS itm) ON (sup.id = itm.supplier)
    -> LEFT JOIN (jbsale AS sale) ON (sale.item = itm.id);
Query OK, 0 rows affected (0,06 sec)

mysql> SELECT * FROM jb.jbsale_supply;
+--------------+-----------------+---------------+----------+
| supplier     | item            | delivered_qty | sold_qty |
+--------------+-----------------+---------------+----------+
| Cannon       | Wash Cloth      |           575 |     NULL |
| Levi-Strauss | Bellbottoms     |           600 |     NULL |
| Playskool    | ABC Blocks      |           405 |     NULL |
| Whitman's    | 1 lb Box        |           100 |        2 |
| Whitman's    | 2 lb Box, Mix   |            75 |     NULL |
| Fisher-Price | Maze            |           200 |     NULL |
| White Stag   | Jacket          |           300 |        1 |
| White Stag   | Slacks          |           325 |     NULL |
| Playskool    | Clock Book      |           150 |        2 |
| Fisher-Price | The 'Feel' Book |           225 |     NULL |
| Cannon       | Towels, Bath    |          1000 |        5 |
| Fisher-Price | Squeeze Ball    |           400 |     NULL |
| Cannon       | Twin Sheet      |           750 |        1 |
| Cannon       | Queen Sheet     |           600 |     NULL |
| White Stag   | Ski Jumpsuit    |           125 |        3 |
| Levi-Strauss | Jean            |           500 |     NULL |
| Levi-Strauss | Shirt           |          1200 |        1 |
| Levi-Strauss | Boy's Jean Suit |           500 |     NULL |
+--------------+-----------------+---------------+----------+



