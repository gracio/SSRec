import redis
import pid2cat
import sys

r = redis.StrictRedis(host = "localhost")

catname = sys.argv[1]
quiz_file = sys.argv[2]

f = open(quiz_file)
for i in f:
	print pid
	pid = i.strip()
	brandcat = pid2cat.pid2cat(i)
	brandcat = 'cat=' + str(brandcat[1]) + '&fl=b' + str(brandcat[0])
	r.hset("SS:Recommendations:quiz:" + catname, pid , brandcat)


# catname = "cat109"
# quiz = ['251379999', '444704201', '327482514', '343527540', '442993666', '254465872', '361265129', '431387044', '277627106', '254347425', '445915990', '251492272', '351551809', '380815266', '254163192', '287513377', '364057509', '262960565', '447048957', '363709083', '35554451', '447637285', '381653074', '362024011', '280094543']

# r.delete("SS:Recommendations:quiz:" + catname)
# for i in quiz:
# 	brandcat = pid2cat.pid2cat(i)
# 	brandcat = 'cat=' + str(brandcat[1]) + '&fl=b' + str(brandcat[0])
# 	r.hset("SS:Recommendations:quiz:" + catname,i , brandcat)


# catname = "cat219"
# quiz = ['286692815','319157615','317279147','148428451','276742962','289186201','300874784','254373775','289122855', '298012330', '305361780', '148419355', '186352019', '254158024', '271262559','308797126','304059498','298112745','264937690','275194217','177128586','254345209','148430598','317439509','254436428']

# r.delete("SS:Recommendations:quiz:" + catname)
# for i in quiz:
# 	brandcat = pid2cat.pid2cat(i)
# 	brandcat = 'cat=' + str(brandcat[1]) + '&fl=b' + str(brandcat[0])
# 	r.hset("SS:Recommendations:quiz:" + catname,i , brandcat)