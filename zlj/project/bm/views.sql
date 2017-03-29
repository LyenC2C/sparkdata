DROP VIEW "BM_SPM_V";
CREATE VIEW "BM_SPM_V"
 ( "CATNUM","MODEL","LANG","SEQNUM","FRONTAX","STEER","REARAX","CONTFLG","A_BODY","DIST","ENGINE","PLATFRM","AUTO","SALESD","NUMRECS","MANUAL","TEXT","FUELCELL","HVBATTERY","EMOTOR","EXHAUSTSYS" )
 AS 
SELECT catnum, model, lang, seqnum, frontax, steer, rearax, contflg, a_body, dist, engine, platfrm, auto, salesd,
numrecs, manual, text, fuelcell, hvbattery, emotor, exhaustsys FROM bm_spm;
GRANT SELECT ON "BM_SPM_V" TO "TBUSER" ;


DROP VIEW "MODELS_V";
CREATE VIEW "MODELS_V"
 ( "CLASS","AGGTYPE","APPINF","TYPE","SUBBM1","SUBBM2","CATNUM","SALESDES","DATEF","CATTYP2","SUBBM3","SOURCE","DATET","REMARKS","CATTYPE","MODELYR" )
 AS
SELECT class, TRIM(aggtype) as aggtype, appinf, type, subbm1, subbm2, catnum, salesdes, datef, cattyp2, subbm3, source, datet,
remarks, cattype, modelyr FROM newbms;
GRANT SELECT ON "MODELS_V" TO "TBUSER" ;


DROP VIEW "BM_GROUP_V";
CREATE VIEW "BM_GROUP_V"
 ( "CATNUM","GROUPNUM","ILLCNT","DESCIDX" )
 AS
SELECT catnum, groupnum, illcnt, descidx FROM bm_group;
GRANT SELECT ON "BM_GROUP_V" TO "TBUSER" ;


DROP VIEW "BM_SUBGRP_V";
CREATE VIEW "BM_SUBGRP_V"
 ( "CATNUM","GROUPNUM","LANG","SUBGRP","TEXT" )
 AS
SELECT catnum, groupnum, lang, subgrp, text FROM bm_subgrp;
GRANT SELECT ON "BM_SUBGRP_V" TO "TBUSER" ;

DROP VIEW "BM_PARTS2_V";
CREATE VIEW "BM_PARTS2_V"
 ( "CATNUM","GROUPNUM","SUBGRP","SEQNUM","SEQNO","CALLOUT","REPLFLG","PARTTYPE","PARTNUM","NOUNIDX","DESCIDX","INDENT","TUVSIGN","OPTFLAG","CHANGEFLAG","STEERING","TYPE","REPTYPE","REPPNO","FOOTNOTES","NEUTRAL","CODEB","QUANTBM","COMPONENTS","SUBMODS","REPPART","OPTPART","DAMAGE_PART","RANKING_NUMBER","REPPREM","PARTREM","POS_ADDR","VPD_IDENT" )
 AS
SELECT catnum, groupnum, subgrp, SEQNUM, seqno, callout, replflg, parttype, partnum, nounidx, descidx, indent, tuvsign,
	optflag, changeflag, steering, type, reptype, reppno, footnotes, neutral, codeb, quantbm, replace ('EVO' by 'BK ' in components) as components, submods, reppart,
	optpart, damage_part,ranking_number, repprem, partrem, pos_addr, vpd_ident
FROM BM_PARTS;
GRANT SELECT ON "BM_PARTS2_V" TO "TBUSER" ;
