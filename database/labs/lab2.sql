CREATE TABLE jbmanager
    (
    `id` INT(11),
    `bonus` INT(11) NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    FOREIGN KEY (`id`) REFERENCES `jbemployee`(`id`)
    );
        
INSERT INTO jb.jbmanager(`id`)
    SELECT * 
    FROM (SELECT manager FROM jbemployee
          UNION
          SELECT manager FROM jbdept) AS mngr
	WHERE mngr.manager IS NOT NULL;
   
ALTER TABLE `jbemployee` 
DROP FOREIGN KEY `fk_emp_mgr`;

ALTER TABLE `jbemployee`
ADD CONSTRAINT `fk_emp_mgr`
FOREIGN KEY (`manager`) REFERENCES `jbmanager` (`id`);

ALTER TABLE `jbdept` 
DROP FOREIGN KEY `fk_dept_mgr`;
                                 
ALTER TABLE `jbdept`
ADD CONSTRAINT `fk_dept_mgr`
FOREIGN KEY (`manager`) REFERENCES `jbmanager` (`id`);

SET SQL_SAFE_UPDATES = 0;
UPDATE `jbmanager` 
  SET bonus = bonus + 10000 
  WHERE id IN (SELECT DISTINCT(manager) FROM jb.jbdept);
SET SQL_SAFE_UPDATES = 1;  

SELECT * FROM jb.jbmanager;
/*
+-----+-------+
| id  | bonus |
+-----+-------+
|  10 | 10000 |
|  13 | 10000 |
|  26 | 10000 |
|  32 | 10000 |
|  33 | 10000 |
|  35 | 10000 |
|  37 | 10000 |
|  55 | 10000 |
|  98 | 10000 |
| 129 | 10000 |
| 157 | 10000 |
| 199 |     0 |
+-----+-------+
12 rows in set (0,00 sec)
*/

