"""
this script is used for save history data

more specifically: 
  1. save modified content to history table, this is for extra backup
  2. move all content from table `bbs_758` to `bigbbs_758`

It's recommended to run this script before a search for the whole database or routinely executed

By the way, the table I used for search is created by using Mysql MyISAM Merge Engine, remember to modify the bigbbs table list:

CREATE TABLE `data` (
  `id` int(11) NOT NULL,
  `lc` int(255) NOT NULL,
  `posttime` datetime NOT NULL,
  `edittime` datetime NOT NULL,
  `user` varchar(66) NOT NULL,
  `content` longtext NOT NULL,
  `gettime` datetime NOT NULL,
  PRIMARY KEY (`id`,`lc`,`edittime`,`posttime`,`user`),
  KEY `a1` (`posttime`),
  KEY `a2` (`user`),
  KEY `a3` (`gettime`),
  KEY `a4` (`id`),
  KEY `a5` (`lc`),
  KEY `a6` (`edittime`)
) ENGINE=MRG_MyISAM DEFAULT CHARSET=utf8mb4 UNION=(`bigbbs_100`,`bigbbs_758`,`bigbbs_...`); #! change here to your bigbbs table list!
"""

from config import db
conn = db()

def runsql(sql):
    global conn
    conn=db()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        print("Error:")
        print(e)
        
thesql = """
CREATE TABLE if not exists `bigbbs_{id}` (
  `id` int(11) NOT NULL,
  `lc` int(255) NOT NULL,
  `posttime` datetime NOT NULL,
  `edittime` datetime NOT NULL,
  `user` varchar(66) NOT NULL,
  `content` longtext NOT NULL,
  `gettime` datetime NOT NULL,
  PRIMARY KEY (`id`,`lc`,`edittime`,`posttime`,`user`),
  KEY `a1` (`posttime`),
  KEY `a2` (`user`),
  KEY `a3` (`gettime`),
  KEY `a4` (`id`),
  KEY `a5` (`lc`),
  KEY `a6` (`edittime`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4;
insert ignore into history(
    SELECT distinct
        a.id,
        a.lc,
        a.posttime,
        a.edittime,
    a.user,
        a.content
    FROM
        bbs_{id} AS a,
        bbs_{id} AS b
    WHERE
        a.id = b.id
    AND a.lc = b.lc
    AND a.posttime = b.posttime
    AND a.edittime < b.edittime
);
insert ignore into bigbbs_{id} (select * from bbs_{id});
delete from bbs_{id};
"""

# you can modify this list
id_list = ["100", "101", "102", "103", "104", "105", "114", "115", "119", "122", "126", "129", "135", "136", "139", "140", "142", "144", "145", "146", "147", "148", "149", "15", "151", "152", "154", "155", "157", "158", "16", "164", "165", "169", "17", "170", "173", "182", "184", "186", "188", "19", "191", "198", "20", "21", "212", "226", "229", "23", "235", "239", "25", "26", "261", "28", "294", "30", "314", "318", "319", "320", "321", "323", "324", "326", "328", "329", "330", "331", "334", "339", "344", "346", "347", "351", "352", "353", "355", "357", "36", "361", "362", "369", "371", "372", "374", "377", "383", "39", "392", "393", "399", "401", "402", "403", "404", "405", "406", "41", "410", "411", "413", "414", "415", "416", "417", "418", "42", "422", "424", "425", "426", "428", "429", "430", "431", "432", "434", "436", "437", "440", "445", "446", "447", "448", "449", "451", "452", "454", "455", "457", "459", "462", "464", "465", "467", "468", "469", "47", "472", "473", "474", "475", "476", "477", "478", "479", "48", "480", "481", "482", "483", "484", "485", "486", "487", "488", "489", "49", "490", "491", "492", "493", "494", "495", "496", "497", "498", "499", "50", "501", "502", "503", "504", "505", "506", "507", "509", "511", "513", "514", "515", "516", "517", "518", "519", "52", "520", "535", "537", "538", "540", "544", "545", "546", "548", "549", "550", "551", "552", "553", "554", "555", "557", "559", "560", "562", "569", "57", "58", "580", "581", "594", "60", "616", "630", "67", "68", "7", "736", "74", "744", "75", "758", "77", "80", "81", "83", "84", "85", "86", "88", "91", "99"]
for id in id_list:
    print(id)
    runsql(thesql.format(id=id))