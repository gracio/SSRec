import pymysql
import json
from pid2cat import *
import sys
import csv
import cProfile, pstats, StringIO
pr = cProfile.Profile()
pr.enable()

f = open(sys.argv[1])
with open(sys.argv[1] + '_pid2cat.csv','wb') as fw:
	writer = csv.writer(fw)
	for line in f:
		PID = line.strip()
		print PID
		if is_number(PID):
			out = pid2cat(PID)
			if out[-1] == 'ok':
				writer.writerow(out)

pr.disable()
s = StringIO.StringIO()
sortby = 'cumulative'
ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
ps.print_stats()
print s.getvalue()

# f = open('items.json')
# PIDs = []
# for idx, i in enumerate(f):
# 	print idx
# 	PID = json.loads(i)['_id']
# 	PID = PID[2:]
# 	currentType = json.loads(i)['itypes']
# 	if not currentType[0] == 'retired':
# 		cat, status, tag, exit_flag = pid2cat(PID)
# 	else:
# 		print 'retired'
# f.close()


# def pid2cat_archive(PID):

# 	level1cat = { 2: 'women', 166: 'men', 323: 'kids & baby', 806: 'living' }
# 	level2cat = {'women': { 109 : ['women','shoes'], 544 : ['women','clothes'], 31 : ['women','handbags'], 413 : ['women','beauty'] },
# 	'men': { 219 : ['men','shoes'], 1741 : ['men','clothes'], 168 : ['men','bags'], 172 : ['men','beauty']},
# 	'kids & baby' : {}, 'living' : {}}
# 	category_dict_full = dict(level1cat,**reduce(lambda acc,form: dict(acc,**form),level2cat.values()))

# 	tag = []
# 	category_node_id = []
# 	exit_flag = 'ok'
# 	cat_match = []
# 	parent_match = []
# 	level2parent_match = []
# 	cat = []
# 	cat_name = []
# 	score = []
# 	default_cat = []

# 	conn = pymysql.connect(host='127.0.0.1', port=3306, user='shopstyle_view', passwd='born2run', db='shopstyle_prod')  
# 	cur = conn.cursor(pymysql.cursors.DictCursor)
# 	success = cur.execute("select product.name, product.status, category_node.tag, category_node.category_node_id, category_node.parent_id \
# 	from product_tag join category_node on product_tag.category_tag = category_node.tag \
# 	join product on product_tag.product_id = product.product_id and product.uber_id = " + PID) 

# 	if success:
# 		retrieved = cur.fetchall()
# 		status = retrieved[0]['status']
# 		name = retrieved[0]['name']
# 		for j in retrieved:
# 			tag.append(j['tag'])
# 			category_node_id.append(j['category_node_id'])

# 		if not tag:
# 			exit_flag = '! no tags available'
# 			print 'exit_flag:', exit_flag
# 			cur.close()
# 			return 'nan', 'nan', 'nan', exit_flag
		
# 		tag_set = set(tag)
# 		tag_set = [i.lower() for i in tag_set]

# 		eligible_dict = {}
# 		for k in level2cat.keys():
# 			if k in tag_set: # if there is a level1cat match

# 				if level2cat[k] == {}: # if no further category to check, return 
# 					exit_flag = 'top level match'
# 					cat = [ level1cat.keys()[level1cat.values().index(k)] ] # return this top level category
# 					return cat, status, tag_set, exit_flag

# 				for kk in level2cat[k].keys():
# 					if not set(level2cat[k][kk]).difference(tag_set): # if there is a full match in this level2 category, return this category
# 						exit_flag = 'full match'
# 						cat = [kk]
# 						cat_name = category_dict_full[kk]
# 						print 'exit_flag:', exit_flag, '. PID:', PID, '. name:', name, '. assigned to:', cat, cat_name, '. tags:' , tag_set, '. status: ', status
# 						# return the category
# 						return cat, status, tag_set, exit_flag

# 				# otherwise, filter match only. perform ancestry search on filtered dict to infer category
# 				eligible_dict = dict(eligible_dict, **level2cat[k])
# 				cat = [ level1cat.keys()[level1cat.values().index(k)] ] # becomes the default unless overwritten


# 		if eligible_dict == {}: # if no matches at all to the categories:
# 			eligible_dict = category_dict_full # perform ancestry search on full dict 	

# 		# get level 2 parent for each tag	
# 		level2parent =[levelNcategory(p,2)[0] for p in category_node_id]

# 		zipper = zip(tag, level2parent)

# 		candidate_cats = set(level2parent).intersection(eligible_dict.keys())
# 		if len(set(candidate_cats).difference(level1cat.keys())) > 1:
# 			exit_flag = '! multiple matches'

# 		candidate_cats = list(candidate_cats)
# 		if not candidate_cats:
# 			exit_flag = '! no candidate cats'
# 		else:
# 			score = []
# 			for c in candidate_cats:
# 				score.append(len(set([z[0] for z in zipper if z[1] == c])))
# 			if max(score) > 0:# if there's valid max score
# 				cat = [candidate_cats[idx] for idx,i in enumerate(score) if i == max(score)]
# 			else: 
# 				exit_flag = '! no score match' 	

# 		if cat:
# 			for c in cat:
# 				if type(category_dict_full[c]) == str:
# 					cat_name = category_dict_full[c]
# 				else:
# 					cat_name.append('-'.join(category_dict_full[c]))		

# 		print 'exit_flag:', exit_flag, '. PID:', PID, '. name:', name, '. assigned to:', cat, cat_name, '. tags:' , tag_set, '. status: ', status, '. candidate_cats:', candidate_cats, '. scores:', score
# 		cur.close()
# 		return cat, status, tag_set, exit_flag
# 	else:
# 		exit_flag = '! unavailable from database'
# 		print 'exit_flag:', exit_flag
# 		cur.close()
# 		return 'nan', 'nan', 'nan', exit_flag

