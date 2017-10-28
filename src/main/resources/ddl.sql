-------------------hive-----------------
CREATE TABLE lkl_card_score.one_degree_data(
    c0 string,
    c1 string,
    c2 string,
    c3 string,
    c4 string,
	  c5 string,
	  c6 string)
PARTITIONED  by(year int,month int,day int);


CREATE TABLE lkl_card_score.two_degree_data(
    c0 string,
    c1 string,
    c2 string,
    c3 string,
    c4 string,
	  c5 string,
	  c6 string,
	  c7 string,
	  c8 string,
	  c9 string,
	  c10 string)
PARTITIONED  by(year int,month int,day int);


CREATE TABLE lkl_card_score.graphx_tansported_ordernos(
    c0 string,
    c1 string)
PARTITIONED  by(year int,month int,day int);
-------------------mysql-----------------
CREATE TABLE EXCEPTION_ORDERNO_TBL(
  ORDERNO varchar(50) NOT NULL COMMENT '订单号',
  INSERT_TIME timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '时间',
  EX_TYPE char(5) COMMENT '异常类型分别有三种类型：a超过大小限制，b孤点，c白名单'
);

CREATE TABLE EXCEPTION_ORDERNO_TBL2VAR(
  ORDERNO varchar(50) NOT NULL COMMENT '订单号',
  INSERT_TIME timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '时间',
  EX_TYPE char(5) COMMENT '异常类型分别有三种类型：a超过大小限制，b孤点，c白名单'
);

