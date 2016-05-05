hive<<EOF

use wlbase_dev;

create table wlservice.t_tc_shigeshangpindaysale as
select *
from wlbase_dev.t_base_ec_item_daysale_dev
where item_id = '22565888316 '   or	item_id = '520166234397 '   or	item_id =
'45513761373 '   or	item_id = '521116561201 '   or	item_id = '524207473858'
  or	item_id = '35657960213 '   or	item_id = '524830071189 '   or	item_id= '524028757395 '
  or	item_id = '44489648450 '   or	item_id = '44717228911 'or	item_id = '523935690985 '
  or	item_id = '526165160946 '   or	item_id = '37616151176 '   or	item_id = '36944367817 '
  or	item_id = '522693120251'   or	item_id = '45536521853 '   or	item_id = '44048434417 '
  or	item_id = '530776711316 '   or	item_id = '40028688104 '   or	item_id = '520112372605'
  or	item_id = '26707248461 '   or	item_id = '520617779997 '   or	item_id= '36825963873 '
  or	item_id = '35076606123 '   or	item_id = '525071406461'   or	item_id = '44048690217 '
  or	item_id = '37617697416 '   or	item_id= '43780782188 '   or	item_id = '39227753979 '
  or	item_id = '2768948236 'or	item_id = '529561572845 '   or	item_id = '40014906657 '
  or	item_id= '527904541630 '   or	item_id = '525256947263 '   or	item_id = '37365840175 '
  or	item_id = '40666582353 '   or	item_id = '528089973295 '   or	item_id= '524058338861 '
  or	item_id = '527685632977 '   or	item_id = '41384617050 ' or	item_id = '15308802512 '
  or	item_id = '527853750972 '   or	item_id= '40714497780 '   or	item_id = '525257754147 '   or	item_id = '9109694871 '
or	item_id = '40685463839 '   or	item_id = '522933653508 '   or	item_id
= '521993353085 '   or	item_id = '527670465845 '   or	item_id = '40651743155 '
or	item_id = '526545699377 '   or	item_id = '527904381060 '   or	item_id
= '526934971607 '   or	item_id = '528055001368 '   or	item_id = '43277859972 '
or	item_id = '521603040665 '   or	item_id = '44201319455 '   or	item_id
= '40666442184 '   or	item_id = '523939780043 '   or	item_id = '40014602022 '
or	item_id = '526149430218 '   or	item_id = '526405802834 '   or	item_id
= '44656722570 '   or	item_id = '40752576772 '   or	item_id = '42611531664 '
or	item_id = '40722181721 '   or	item_id = '530416464281 '   or	item_id
= '45339863085 '   or	item_id = '527084147175 '   or	item_id = '526144910622'   or	item_id = '529273879597 '   or	item_id = '524403035763 '   or	item_id
= '45651566643 '   or	item_id = '18710808587 '   or	item_id = '528624232419'   or	item_id = '523279281025 '   or	item_id = '40176801370 '   or	item_id
= '526164140644 '   or	item_id = '528957793373 '   or	item_id = '41845640294 '
or	item_id = '42018189861 '   or	item_id = '524935492359 '   or	item_id
= '44260188683 '   or	item_id = '529092100909 '   or	item_id = '37638996029 '
or	item_id = '43830746784 '   or	item_id = '45595600693 '   or	item_id
= '525226167865 '   or	item_id = '520847099251 '   or	item_id = '38170487507 '
or	item_id = '45590973433 '   or	item_id = '40701446910 '   or	item_id
= '41247376088 '   or	item_id = '526332576444 '   or	item_id = '16292753869 '
or	item_id = '527900943396 '   or	item_id = '527909377755 '   or	item_id
= '37113926101 '   or	item_id = '520942294121 '   or	item_id = '529008157763'   or	item_id = '43695194122 '   or	item_id = '526284225508 '   or	item_id
= '522702718572 '   or	item_id = '40143647602 '   or	item_id = '43346076068 '
or	item_id = '524670707593 '   or	item_id = '526204663613 '   or	item_id
= '40332821991 '   or	item_id = '526605644284 '   or	item_id = '526545515873'   or	item_id = '526603216064 '   or	item_id = '45432563648 '   or	item_id
= '23051164150 '   or	item_id = '9109808171 '   or	item_id = '527905409587'   or	item_id = '526241354115 '   or	item_id = '37104174547 '   or	item_id
= '40589231423 '   or	item_id = '40858316303 '   or	item_id = '527344383367'   or	item_id = '41543788438 '   or	item_id = '526475433571 '   or	item_id
= '523249838771 '   or	item_id = '42597836782 '   or	item_id = '525701229354'   or	item_id = '525862551303 '   or	item_id = '527330770649 '   or	item_id
= '44375832379 '   or	item_id = '520114478597 '   or	item_id = '42120082582 '
or	item_id = '526049166476 '   or	item_id = '527562214945 '   or	item_id
= '520928380305 '   or	item_id = '44893229113 '   or	item_id = '520270725162'   or	item_id = '520370572308 '   or	item_id = '521405473035 '   or	item_id
= '520516910270 '   or	item_id = '42221948182 '   or	item_id = '45554313899 '
or	item_id = '529341681796 '   or	item_id = '526113399856 '   or	item_id
= '520132905330 '   or	item_id = '44954462769 '   or	item_id = '523792200193'   or	item_id = '44045295940 '   or	item_id = '44172468762 '   or	item_id
= '520170738432 '   or	item_id = '523072706024 '   or	item_id = '521472268095'   or	item_id = '521471142384 '   or	item_id = '521471010307 '   or	item_id
= '520432205593 '   or	item_id = '529614503622 '   or	item_id = '523960189512'   or	item_id = '522688321875 '   or	item_id = '520146760714 '   or	item_id
= '520430983848 '   or	item_id = '521056410295 '   or	item_id = '520134079615'   or	item_id = '524596450376 '   or	item_id = '529518837339 '   or	item_id
= '43705593874 '   or	item_id = '527164002207 '   or	item_id = '521849268691'   or	item_id = '521472798265 '   or	item_id = '43859161526 '   or	item_id
= '530147259458 '   or	item_id = '44214125511 '   or	item_id = '521514125382'   or	item_id = '528178048305 '   or	item_id = '43811607293 '   or	item_id
= '524136456239 '   or	item_id = '529606350925 '   or	item_id = '525151373226'   or	item_id = '530216902909 '   or	item_id = '523786561135 '   or	item_id
= '45031530672 '   or	item_id = '524422548479 '   or	item_id = '527164426499'   or	item_id = '522088661309 '   or	item_id = '20140464196 '   or	item_id
= '530711943740 '   or	item_id = '520279305746 '   or	item_id = '529515437836'   or	item_id = '525115155484 '   or	item_id = '530788373416 '   or	item_id
= '525064798644 '   or	item_id = '527725188681 '   or	item_id = '521413351381'   or	item_id = '520272236157 '   or	item_id = '44197583729 '   or	item_id
= '45417782526 '   or	item_id = '520055026889 '   or	item_id = '526045462506'   or	item_id = '529663639426 '   or	item_id = '527695610075 '   or	item_id
= '520156508763 '   or	item_id = '520154999583 '   or	item_id = '527994929315'   or	item_id = '530214631000 '   or	item_id = '520003834722 '   or	item_id
= '44827617731 '   or	item_id = '522564240776 '   or	item_id = '520956033265'   or	item_id = '522872978978 '   or	item_id = '529651344910 '   or	item_id
= '525115139844 '   or	item_id = '522960193532 '   or	item_id = '522161122343'   or	item_id = '43800119848 '   or	item_id = '523073454272 '   or	item_id
= '520002032578 '   or	item_id = '527393185036 '   or	item_id = '520530555339'   or	item_id = '524461138157 '   or	item_id = '521262210466 '   or	item_id
= '527051064617 '   or	item_id = '45066946981 '   or	item_id = '524461765765'   or	item_id = '44516210076 '   or	item_id = '521365961073 '   or	item_id
= '41631005986 '   or	item_id = '523403778395 '   or	item_id = '45882321989 '
or	item_id = '45255649632 '   or	item_id = '521382092447 '   or	item_id
= '521186506707 '   or	item_id = '44557457884 '   or	item_id = '528675342062'   or	item_id = '44056818966 '   or	item_id = '44989036442 '   or	item_id
= '524431007205 '   or	item_id = '522730485911 '   or	item_id = '529450496058'   or	item_id = '524462265939 '   or	item_id = '522642972798 '   or	item_id
= '42890625758 '   or	item_id = '42439471504 '   or	item_id = '35019422158 '
or	item_id = '523405037590 '   or	item_id = '521254969747 '   or	item_id
= '523169302961 '   or	item_id = '521670341037 '   or	item_id = '526368948013'   or	item_id = '526311571617 '   or	item_id = '526365824683 '   or	item_id
= '526354817540 '   or	item_id = '529700348851 '   or	item_id = '522042672182'   or	item_id = '523780565537 '   or	item_id = '521474110739 '   or	item_id
= '520136931482 '   or	item_id = '44214243984 '   or	item_id = '523161997047'   or	item_id = '523272377949 '   or	item_id = '523167385007';

EOF