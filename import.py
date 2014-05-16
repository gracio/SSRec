import predictionio
import sys
import json
# arguments: pid_file, u2ifile, brand-cat-option, label

key = "c9E6ehRTIIRUs3ZLgqXjsXWyr6CSGKGiHRoWINzk2POfzcjWSyeoglEeQVmWYfkK"
client = predictionio.Client(key,qsize=1000)

pid_file = sys.argv[1]
u2i_file = sys.argv[2]
brand_cat_option = sys.argv[3]
label = sys.argv[4]
pid_file_brand_idx = 6
pid_file_pid_idx = 0
u2i_file_pid_idx = 3
u2i_file_uid_idx = 4

if brand_cat_option == '1':
	print "use brand_cat_option 1"
	cat_idx = 4
elif brand_cat_option == '2':
	print "use brand_cat_option 2"
	cat_idx = 3
elif brand_cat_option == '3':
	print "use brand_cat_option 3"
	cat_idx = 2


pid_list = []
brand_cat = []
pid_f = open(pid_file)
for p in pid_f:
	pp = p.split(',')
	pid_list.append(pp[pid_file_pid_idx].strip()) # PID 
	brand_cat.append('cat=' + str(pp[cat_idx]) + '&fl=b' + str(pp[pid_file_brand_idx]) )

print 'done reading pid_list', pid_list[:10], brand_cat[:10]

u2i_f = open(u2i_file)
for fdx,f in enumerate(u2i_f):
	try:
		d = json.loads(f)
		uid = d['uid'][d['uid'].index('_')+1:].strip()
		pid = d['iid'][d['iid'].index('_')+1:].strip()
	except:
		u2i = f.split(',')
		uid = u2i[u2i_file_uid_idx].strip()
		pid = u2i[u2i_file_pid_idx].strip()
	print fdx
	if pid in pid_list:
		print fdx, pid
		if brand_cat_option < 0:
			item = pid
		else:
			item = brand_cat[pid_list.index(pid)]
		try:
			client.create_item(item, (label,))
			client.create_user(uid)
			client.identify(uid)
			client.record_action_on_item("like", item)
			print 'marked as ' + label, item
		except:
			'trouble with ', item
