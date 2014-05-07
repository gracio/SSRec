def levelNcategory(category_node_id,levelN):
    import pymysql 
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='shopstyle_view', passwd='born2run', db='shopstyle_prod')  
    cur = conn.cursor(pymysql.cursors.DictCursor)
    ownName = 'na'
    ownLevel = -1
    levelNcategoryName = 'na'
    levelNID = int(category_node_id)
    #print category_node_id
    #cur.close()
    #conn = pymysql.connect(host='127.0.0.1', port=3306, user='shopstyle_view', passwd='born2run', db='shopstyle_prod')  
    #cur = conn.cursor(pymysql.cursors.DictCursor)
    success = cur.execute('select parent_id, depth, identifier from category_node where category_node_id = ' + str(category_node_id))
    if success:
        retrieved = cur.fetchall()
        #print retrieved
        ownName = retrieved[0]['identifier']
        ownLevel = retrieved[0]['depth']
        if retrieved[0]['depth'] == levelN:
            levelNcategoryName = retrieved[0]['identifier']
        elif retrieved[0]['depth'] < levelN:
            levelNcategoryName = 'na'
        else:
            if retrieved[0]['parent_id']:
                #print 'checking out node '+ str(retrieved[0]['parent_id']) + ' level ' + str(retrieved[0]['depth']-1) + ' levelN=' + str(levelN)
                levelNID, levelNcategoryName, tp, tp2 = levelNcategory(str(retrieved[0]['parent_id']), levelN)
    cur.close()
    return levelNID, levelNcategoryName, ownLevel, ownName

def tanimoto(list1, list2):
	set1 = set([i.lower() for i in list1])
	set2 = set([i.lower() for i in list2])
	return len(set1.intersection(set2))/float(len(set1.union(set2)))

def pid2cat(PID, levelN = -1):
	import pymysql
	PID = str(PID)
	# grab tags for PID
	# first hard match against men, women, others
	# next, within the root level category, find best word match

	score = []
	exit_flag = 'ok'
	level2parent = []

	conn = pymysql.connect(host='127.0.0.1', port=3306, user='shopstyle_view', passwd='born2run', db='shopstyle_prod')  
	cur = conn.cursor(pymysql.cursors.DictCursor)

	# get product tags
	success_tag = cur.execute("select product_tag.category_tag, product.brand_id, product.price, product.status from product_tag join product on product.product_id = product_tag.product_id and product.uber_id = " + PID)
	if success_tag:
		retrieved = cur.fetchall()
		tag_set = []
		status = retrieved[0]['status']
		brand = retrieved[0]['brand_id']
		price = retrieved[0]['price']
		for j in retrieved:
			tag_set.append(j['category_tag'])
		tag_set = set(tag_set)
		tag_set = [i.lower() for i in tag_set]
	else:
		exit_flag = '! tag_set unavailable from database'
		#print 'exit_flag:', exit_flag
		cur.close()
		if levelN < 0:
			return PID, None,None,None,None,None, None, None, None, None, None, exit_flag
		else:
			return None, None

	# get candidate category_nodes matching the tags
	success_catnode = cur.execute("select identifier, category_node_id, parent_id, depth from category_node where deleted = 00000000 ") 
	if success_catnode:
		retrieved = cur.fetchall()
		category_node_id = []
		identifier = []
		parent_id = []
		depth = []
		for j in retrieved:
			category_node_id.append(j['category_node_id'])
			identifier.append(j['identifier'])
			parent_id.append(j['parent_id'])
			depth.append(j['depth'])
	else:
		exit_flag = '! catnode unavailable from database'
		#print 'exit_flag:', exit_flag
		cur.close()
		if levelN < 0:
			return PID, None,None,None,None,None, None, None, None, None, None, exit_flag
		else:
			return None, None

	# if info from SSmysql is empty, exit
	if not tag_set or not identifier or not category_node_id:
		exit_flag = '! no tags, identifier, or category_node_id available'
		#print 'exit_flag:', exit_flag
		cur.close()
		if levelN < 0:
			return PID, None,None,None,None,None, None, None, None, None, None, exit_flag
		else:
			return None, None

	# calculate matching score between product tags and candidate category identifiers
	for j in identifier:
		score.append(tanimoto(tag_set, j.split()))

	if max(score) > 0:
		matches_id = [idx for idx, i in enumerate(score) if i == max(score) and parent_id[idx]]
		#print matches_id, parent_id[idx]
		if matches_id:
			match_id = matches_id[0]
			cat = category_node_id[match_id]
			identifier = identifier[match_id]
			if levelN > 0:
				levelNparent, levelNcategoryName, ownLevel, ownName = levelNcategory(cat,levelN)
			else:
				level1parent, levelNcategoryName, ownLevel, ownName = levelNcategory(cat,1)
				level2parent, levelNcategoryName, ownLevel, ownName = levelNcategory(cat,2)
				level3parent, levelNcategoryName, ownLevel, ownName = levelNcategory(cat,3)
		else:
			matches_id = [idx for idx, i in enumerate(score) if i == max(score)]
			match_id = matches_id[0]
			cat = category_node_id[match_id]
			identifier = identifier[match_id]
			level3parent, levelNcategoryName, ownLevel, ownName = levelNcategory(cat,3)
			if levelN == 3:
				levelNparent = level3parent
			else:
				level2_idx = [idx for idx, i in enumerate(depth) if i == 2]
				max_level2_score = max([score[i] for i in level2_idx])
				level2parent_idx = [i for i in level2_idx if score[i] == max_level2_score and parent_id[i]]
				if level2parent_idx:
					level2parent = category_node_id[level2parent_idx[0]]
					level1parent = parent_id[level2parent_idx[0]]
				else:
					level2parent = cat
					level1parent = cat
				if levelN == 2:
					levelNparent = level2parent
				else:
					levelNparent = level1parent
	cur.close()
	tags = '-'.join(tag_set)

	if levelN < 0:
		return PID, cat, level3parent, level2parent, level1parent, ownLevel, brand, price, identifier, status, tags, exit_flag
	else:
		return brand, levelNparent

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
