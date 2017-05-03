#coding:utf-8
/*
segmentation:HanLP
created on:2017-04-10
shell-path:/home/hadoop/common/hanlp
spark-shell --driver-memory 6g --executor-memory 15g --executor-cores 5 --num-executors 15 --jars hanlp-1.2.9.jar
data :/hive/warehouse/wl_analysis.db/t_lt_record_base_title_all_label
*/

//scala segment words
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.utility.Predefine

1. -- segment words only
val text = sc.textFile("/hive/warehouse/wl_service.db/t_lt_base_sp_item_fraud_train_v3/*").map{ x =>
      Predefine.HANLP_PROPERTIES_PATH = "/home/hadoop/common/hanlp/hanlp.properties"
      var sTemp=x.split("\001")
      val temp = HanLP.segment(sTemp(1))
      CoreStopWordDictionary.apply(temp)
      val word = for (i <- Range(0, temp.size())) yield temp.get(i).word
      //val Bi_words = for (i <- Range(0, word.length-1)) yield word(i).concat(word(i+1))
      //val all_words = word ++ Bi_words
      //word.mkString(" ")
      sTemp(0)+"\t"+word.mkString(" ")+"\t"+sTemp(word.length-1)
    }

text.saveAsTextFile("/user/lt/Hanlp_fenci1")


2.-- segment words and filter fraud title
import scala.util.matching.Regex
import org.apache.spark.SparkContext
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.utility.Predefine

sc = SparkContext(appName="labelAndSegment")
val keywords_1 = sc.broadcast("纠纷|坏账|呆账|欠条|瑞银信|破人脸|秒过人脸|法律纠纷|债权债务|陆金所|红岭创投|微贷网|聚宝匯|聚宝汇|宜贷网|爱钱进|宜人贷|团贷网|翼龙贷|你我贷|嘉卡贷|搜易贷|好贷宝|有利网|拿去花|人人贷|温商贷|开鑫贷|博金贷|365易贷|珠宝贷|新新贷|和信贷|连资贷|诚信贷|量化派|微粒贷|拍拍贷|拍拍满|银行流水|抵押|提现|借呗|京东白条|平安易贷|任性付|易贷网|不良记录|征信|黑户|解套回款|身份证抠图|无视黑白|无征信|无视征信|黑户贷款|黑户下卡|贷款包装|纯白户面皮|白户贷款|电核流水|征信代打|无视网黑|代做企业对公流水账|代做普通流水账|工资流水|包过电核|企业邮箱|单位邮箱|资金周转|借钱|典当行|放贷|还款|拆借|快借|负债|催收|资金困难|资金短缺|联贷|银行信贷|快钱|借条|信贷资金|本息|消费信贷|快易花|亲亲小贷|马上贷|51人品贷|瞬时贷|随心贷|广发零用钱|现金巴士|极速贷|手机贷|租房贷|用钱宝|小秒钱包|58消费贷|闪电借款|宜人极速贷|卡卡贷|卡拉卡|有用分期|玖富|小微贷|飞贷|大学分期|民工分期|蓝领分期|好贷|催款函|答辩状|罚单|反诉|房产纠纷|行政复议|行政诉讼|合同纠纷|老赖|欠款|强制执行|取保候审|上诉书|上诉状|诉讼状|诉状|损害赔偿|刑事犯罪|债权|失信|被执行|债务|质押|自诉|打官司|起诉|起诉书|起诉状|遗赠|遗嘱|固话代接|代接回访|代接固话|固话代接|信用卡回访|银行回访|单位包装|贷款回访|回访专用|单位回访|网贷口子|公积金转移|代过人脸识别|代过各种人脸识别|处理逾期|贷款技术|贷款口子|贷款|满标|经营异常|借款逾期|网贷|钱宝网|51人品|一清机|汇付天下|叮当贷|借贷宝|电话回访|撸羊毛|包下款|学历认证|诺诺镑客|卡利宝|限制号出租|站街号|流水回单|汇付天下|营销软件|手机POSS机|手机POS机|手机刷卡机|手机刷卡器|钱袋宝|信用卡刷卡器|移动POS机|强开贷款|电话代接|信用卡贷款|固话回访|薅羊毛|借款培训").value
val keywords_2 = sc.broadcast("时时彩|七乐彩|双色球|大乐透|彩票软件|赌博|中奖符|500万|离婚|财产约定|财产协议|套现|t现|未实名制|网银代缴|网银代付|验证码|抢标|闪电认证|信用卡|无担保|不需担保|秒批|秒到|提额|降额|借款|放款|下款|低息|pos机|急需资金|拿钱|月息|保单|代交|缺钱|得款|审核|大额|燃眉之急|审批|过桥|打卡|银行利息|闪电|计息|正规渠道|代还|通过率|垫资|面签|变现|银行利率|账号|尾款|免息|销户|工资款|闲钱|代打|代开|燃眉|转账|小号|大批量|邮箱|法律顾问|法律文书|法律咨询|法务|合同文书|经济合同|劳动合同|律师函|律师事务所|起草|协议书|續費|入股|高息|违约|典当|诉讼|律师咨询|费率|放米|借贷|公司邮箱|手刷|一清|多商户|低汇率|挂靠|刷卡机|口子|白户|中奖|彩票中奖|实时到账|卡乐付|乐刷|易付宝|网赚|点刷|poss机|ps机|蓝牙POS|蓝牙刷卡机|蓝牙刷卡器|POS刷卡器|刷卡器费率|手机POSS|技术口子|公积金|社保|代缴|贷款|刷芯片卡|秒标|额度|账单技术|收款宝|拉卡拉|跳码|包装小贷|包装公司|呼叫转移").value
val delete_w = sc.broadcast("钱箱|收纳包|保险箱|logo|视频|讲座|PPT|IC卡|读卡器|充值|海报|横幅|考勤|翻译|收银纸|电源|豆瓣|T恤|配件|留学|工作服|充电器|小键盘|网站建站|打印纸|小票纸|热敏|题库|网站建设|天翼|恒昌隆|腊肠|考试|现货|律诗|食堂|考勤|指纹采集仪镜片|师傅|吊顶|简装|家暖|米线|资格考试|代购|套现货|套现代|提现货|套现做|手提现做|信用卡名片夹|黑户外|翼支付|电信|移动|联通|卡贴|信用卡式|信用卡包|信用卡大小|卡片优盘|名片夹|名片盒|卡片盒|卡套|卡盒|卡包|卡袋|卡夹|卡夾|钱包|钱夹|支持信用卡|卡式手机|户外|碎信用卡|卡片刀|名片刀|卡刀|信用卡折叠水果刀").value
val rc_names = Set("生活电器","3C数码配件","本地化生活服务","网店/网络服务/软件","个性定制/设计服务/DIY","办公设备/耗材/相关服务","网络店铺代金/优惠券","网络设备/网络相关","智能设备")

val pattern1 = new Regex(keywords_1)
val pattern2 = new Regex(keywords_2)
val pattern_d = new Regex(delete_w.toUpperCase)

def hanLp(title: String):String = {
    Predefine.HANLP_PROPERTIES_PATH = "/home/hadoop/common/hanlp/hanlp.properties"
    val temp = HanLP.segment(title)
    CoreStopWordDictionary.apply(temp)
    val word = for (i <- Range(0, temp.size())) yield temp.get(i).word
    val Bi_words = for (i <- Range(0, word.length-1)) yield word(i).concat(word(i+1))
    val all_words = word ++ Bi_words
    all_words.mkString(" ")
}

def doLabel(rcname:String,f_score:Int,title:String):Int = {
    var label =0
    if(rc_names.contains(rcname) && f_score >=2){
        val d_words = (pattern1 findAllIn title.toUpperCase).toList.length
        if(d_words == 0){
            label = 1
        }
    }
    label
}

def parse(line: String) ={
    val columns = line.split("\001")
    val title=columns(1).replace(" ","")
    val return_1 = (pattern1 findAllIn title.toUpperCase).toSet
    val return_2 = (pattern2 findAllIn title.toUpperCase).toSet
    var fraud_score = 0
    var keywords = ""
    if(!return_1.isEmpty || !return_2.isEmpty){
        fraud_score = (return_1.toList.length*2 + return_2.toList.length*1)
        keywords = (return_1 ++ return_2).mkString("|")
    }
    val seg_words = hanLp(title)
    val label = doLabel(columns(5),fraud_score,title)
    //selected item: item_id,shop_id,rcname,price,fraud_score,keywords,seg_words
    val return_col = Array(columns(0),columns(15),columns(5),columns(9),fraud_score.toString,keywords,seg_words,label.toString)
    return_col.mkString("\t")

}

val data = sc.textFile("/hive/warehouse/wl_base.db/t_base_ec_item_dev_new/ds=20170410/*")
data.map(x => parse(x)).saveAsTextFile("/user/lt/scala/data_0410")
