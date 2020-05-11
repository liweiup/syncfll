1. 说明
   饭力量项目数据同步 
   4个榜单爬虫部署工作已经完毕，数据已经开始入库。
   百度百科明星人气榜，百度搜索风云榜，寻艺艺人新媒体指数 采集数据放在 188爬虫数据库 
   微博明星势力榜采集数据放在 微博34号爬虫数据库   
   
   
   data_type —— 1:实时数据 2:周榜数据 3:月榜数据',   
   
   #同步规则
   通过项目库中的：super_fans_beta 表中的：tb_star 的：starName
   iis_web_crawler188 -- star_rank
   (实时榜)
   type：1  百度百科明星人气榜  整点后20分钟同步一次到 tb_star_baiduflower中
   type: 2  百度搜索风云榜      0点 、6点、12点、18点 后20分钟同步到 tb_star_baidusearch （data_type）
   type: 3  寻艺榜              0点 后20分钟同步一次到 tb_star_xunyi（data_type 1）
   (周榜)
   type: 4  百度百科明星人气榜   
   type: 5  寻艺榜
   同步规则是找type中最新的一条数据到相应表中
   

    
2. 安装依赖包
    a. 安装依赖包
        pip install -r requirements.txt
        
    b. 升级依赖包到最新版本
        pip install pur
        pur -r requirements.txt
