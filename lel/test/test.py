

# line = '1481990401	542875914150	mtopjsonp1({"api":"com.taobao.idle.item.detail","data":{"item":{"area":"320382:邳州","attributes":"ends:1482239824;yt:105;imageCount:2;pic:640X1136;promotedtype:12583680;textCount:0;","auctionType":"b","barDO":{"actionUrl":"fleamarket://fishpond?id=105","bar":"浙江大学城市学院","barInfo":"result.7","leftIcon":{"height":"15","tagUrl":"https://gw.alicdn.com/tps/TB1R.2aMVXXXXXWXVXXXXXXXXXX-28-28.png","width":"15"}},"bidStatus":"0","browseCount":"25","canBuy":"true","canEditDescription":"true","canPolish":"true","categoryId":"50025445","categoryName":"运动器材","city":"徐州","collectNum":"0","commentNum":"0","commonShareContent":"跟市场价比起来简直八哥价啊！戳进捡漏！#真皮拳击手套#","containsImage":"false","deleteByXiaoer":"false","desc":"感兴趣的话给我留言吧！","descUrl":"http://dsc.taobaocdn.com/i6/540/870/542875914150/TB1L89COFXXXXcsXXXX8qtpFXlX.desc%7Cvar%5Edesc%3Bsign%5Ed12a39ba7f3bd4ad0ea7810a4fd66036%3Blang%5Egb18030%3Bt%5E0","description":"感兴趣的话给我留言吧！","descriptionInfo":"感兴趣的话给我留言吧！","detailFrom":"来自iPhone客户端","divisionId":"320382","favorNum":"0","favored":"false","favoredUserIds":[],"firstModified":"2016-12-16 18:24:29","firstModifiedDiff":"1天前","fishPoolId":"105","fishpoolId":"105","fishpoolName":"浙江大学城市学院","fpSelected":"0","from":"iphone","gps":"34.545819,117.885362","id":"542875914150","imageUrls":["http://img.alicdn.com/bao/uploaded/i3/TB1.Bt1OFXXXXc4aXXXenP6FpXX","http://img.alicdn.com/bao/uploaded/i2/296519963/TB2R8cmXxdkpuFjy0FbXXaNnpXa_!!0-fleamarket.jpg"],"instockByXiaoer":"false","itemCC":"false","itemDeleted":"false","itemStatus":"0","lastAuthorVisitTime":"1481983407431","lastAuthorVisitTimeDiff":"1小时前来过","leftSecond":"0","locationAware":"false","needRecommand":"true","offline":"0","online":"true","originalPrice":"0.00","outStockTime":"2017-03-16 18:24:29","picUrl":"http://img.alicdn.com/bao/uploaded/i4/TB1Zh46OFXXXXXwaXXXenP6FpXX","postPrice":"6.00","price":"280","province":"江苏","resell":"false","secuGuide":{"secuTitle":"什么是闲鱼支付宝担保交易？","secuBody":"https://gw.alicdn.com/tps/TB1mIh4LpXXXXXCaXXXXXXXXXXX-510-394.png","secuBtmUrl":"https://ihelp.taobao.com/pocket/index.htm?psc=28","secuIcon":"https://gw.alicdn.com/tps/TB1xwS9MVXXXXbGXVXXXXXXXXXX-24-24.png","secuBtm":"*闲置商品不支持7天无理由退货服务。请保证在闲鱼平台进行支付，否则闲鱼将无法保证您交易的安全。","secuBtmContext":"点击查看更多知识","secuContent":"担保交易"},"serviceStatus":"0","shortUrl":"http://2.taobao.com/item.htm?id=542875914150","structuredHouse":"false","stuffStatus":"9","subTags":[{"name":"徐州","search":{"province":"江苏","city":"徐州"},"type":"1"},{"name":"浙江大学城市学院","search":{"fishpoolId":"105","pondId":"105"},"type":"3"}],"subscribed":"false","templateId":"2134001330","title":"真皮拳击手套","tradeType":"0","userId":"0","userNick":"cherry谢雨枝","userTagUrlFromServer":"false","videoid":"0","weiboShareContent":"跟市场价比起来简直八哥价啊！戳进捡漏！#真皮拳击手套#","wxurl":"http://www.xianyu.mobi/2shou/appRedirect.html?page=item&id=542875914150","xianyuAbbr":{"abbr":"来闲鱼738天了,卖出过6件宝贝现居徐州邳州，80后摩羯座女生","officialTagList":[{"appearTrackName":"","comment":"实人认证未通过","iconUrl":"https://gw.alicdn.com/tps/TB1TBKcOpXXXXcUapXXXXXXXXXX-32-32.png","link":"https://h5.m.taobao.com/2shou/pd/rearesulterifyUrl.html?userId=296519963&isVerify=0","trackCtrlName":"Button-Shiren","type":"1"},{"appearTrackName":"","comment":"芝麻信用未授权","iconUrl":"https://gw.alicdn.com/tps/TB1resultuGOpXXXXasXFXXXXXXXXXX-32-32.png","link":"ignore=false&userzhima=false&loginuserzhima=false","trackCtrlName":"Button-Zhima","type":"100"}]}},"serverTime":"2016-12-18 00:00:01"},"ret":["SUCCESS::调用成功"],"v":"2.0"})'
# data = line.strip().split('\t')

# print parseJson(getJson(data))
# url = "https://h5.m.taobao.com/2shou/pd/rearesulterifyUrl.html?userId=296519963&isVerify=0"
# params = urlparse(url).query
# kv = {}
# for key_value in params.split('&'):
#     key = key_value.split('=')[0]
#     value = key_value.split('=')[1]
#     kv.setdefault(key, value)
#
# print kv.get("userId")
# print url
# values = url.split('?')[-1]
# for key_value in values.split('&'):
#     print key_value.split('=')[1]
# print values