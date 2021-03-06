DROP TABLE d;
DROP TABLE c;
DROP TABLE b;
DROP TABLE a; -- IF WE DROP a first it will says cannot drop table a because other objects depend on it
create table a (
  id  integer   PRIMARY KEY,
  va1  varchar(40) UNIQUE,
  va2  varchar(40) UNIQUE
);

create table b (
  id integer  PRIMARY KEY,
  vb varchar(40) UNIQUE REFERENCES a(va1) ON UPDATE CASCADE ON DELETE SET NULL
);


create table c (
  id integer  PRIMARY KEY,
  vc varchar(40) UNIQUE REFERENCES a(va2) ON UPDATE CASCADE ON DELETE CASCADE
);

create table d (
  id integer  PRIMARY KEY,
  vd varchar(40) UNIQUE REFERENCES c(vc) ON UPDATE SET NULL ON DELETE CASCADE
);


INSERT INTO a values (0, 'v0', 'v0');
INSERT INTO a values (1, 'v1', 'v1');

INSERT INTO b values (0, 'v0');

INSERT INTO c values (0, 'v0');
INSERT INTO c values (1, 'v1');

INSERT INTO d values (0, 'v0');
INSERT INTO d values (1, 'v1');

SELECT * FROM a;
SELECT * FROM b;
SELECT * FROM c;
SELECT * FROM d;


UPDATE a SET va1 = 'v3' WHERE id = 0;
SELECT * FROM a;
SELECT * FROM b;
SELECT * FROM c;
SELECT * FROM d;
--- expected:   a: 030,111  b:03  c: 00,11  d:00,11

UPDATE a SET va2 = 'v4' WHERE id = 1;
SELECT * FROM a;
SELECT * FROM b;
SELECT * FROM c;
SELECT * FROM d;
--- expected:   a: 030,114  b:03  c: 00,14  d:00,1 null

DELETE FROM a WHERE id = 0;
SELECT * FROM a;
SELECT * FROM b;
SELECT * FROM c;
SELECT * FROM d;
--- expected:   a: 114  b:0 null  c: 14  d:1 null
