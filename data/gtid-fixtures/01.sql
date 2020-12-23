BEGIN;

INSERT INTO `test_db`.`buildings`
VALUES(1, 'ACME Headquaters', '3950 North 1st Street CA 95134');


INSERT INTO `test_db`.`buildings`
VALUES(2, 'ACME Sales', '5000 North 1st Street CA 95134');

COMMIT;
BEGIN;


INSERT INTO `test_db`.`rooms`
VALUES(1, 'Amazon', 1);


INSERT INTO `test_db`.`rooms`
VALUES(2, 'War Room', 1);


INSERT INTO `test_db`.`rooms`
VALUES(3, 'Office of CEO', 1);


INSERT INTO `test_db`.`rooms`
VALUES(4, 'Marketing', 2);


INSERT INTO `test_db`.`rooms`
VALUES(5, 'Showroom', 2);

COMMIT;

BEGIN;

UPDATE `test_db`.`rooms`
SET room_no=4,
    room_name='MARKETING',
    building_no=2
WHERE room_no=4
  AND room_name='Marketing'
  AND building_no=2;


UPDATE `test_db`.`rooms`
SET room_no=5,
    room_name='SHOWROOM',
              building_no=2
WHERE room_no=5
  AND room_name='Showroom'
  AND building_no=2;

COMMIT;

BEGIN;

DELETE
FROM `test_db`.`buildings`
WHERE building_no=1
  AND building_name='ACME Headquaters'
  AND address='3950 North 1st Street CA 95134';

COMMIT;
