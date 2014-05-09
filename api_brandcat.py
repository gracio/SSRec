from flask import Flask, jsonify, abort, request, make_response, url_for
from app import app
import predictionio
import redis
import random
import pid2cat
import urllib2
import json
import urllib2
import json
from datetime import timedelta
from flask import current_app
from functools import update_wrapper
from decorators import async

import logging
from logging.handlers import RotatingFileHandler
file_handler = RotatingFileHandler('logs/SSRecommendationAPI.log', 'a', 1 * 1024 * 1024, 20)
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'))
app.logger.setLevel(logging.INFO)
file_handler.setLevel(logging.INFO)
app.logger.addHandler(file_handler)
app.logger.info('SS Recommendation API startup')







def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, basestring):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator

def process_request(request):
    query = {}
    keys = request.args.keys()
    for k in keys:
        query[k] = request.args.get(k)
    msg = 'processed request to query: '+ str(query)
    app.logger.info(msg)
    return query 

def process_query_string(query_string):
    query = {}
    toks = query_string.split('&')
    for i in toks:
        kv = i.split('=')
        if len(kv) == 2:
            query[kv[0]] = kv[1]
    msg = 'processed query_string to query' + str(query)
    app.logger.info(msg)
    return query 

def check_valid_query(query):
    # check that all parameters are in valid key sets
    valid_keys = set(['uid','cat','likes','dislikes','limit'])
    if set(query.keys()).difference(valid_keys):
        msg = 'keys not in valid sets: ' + ','.join(set(query.keys()).difference(valid_keys))
        app.logger.info(msg)
        abort(400)
    # check that uid and cat are present
    if 'cat' not in query or 'uid' not in query:
        abort(400)
    # convert values to appropriate types
    if 'limit' in query:
        query['limit'] = int(query['limit']) 
    if 'likes' in query:
        query['likes'] = query['likes'].split(',')
    else:
        query['likes'] = []
    if 'dislikes' in query:
        query['dislikes'] = query['dislikes'].split(',')
    else:
        query['dislikes'] = []
    return query

def set_limit(query):
    query['inferred_limit'] = len(query['likes']) + len(query['dislikes'])
    if query['inferred_limit'] > 0:
        query['limit'] = query['inferred_limit'] # overrides limit
    if 'limit' not in query: # if limit is not supplied by user or not inferred
        query['limit'] = 10 #defalt
    if query['limit'] > 25:
        abort(400)
    return query

def valid_initial_call(query):
    return 'dislikes' not in query and 'likes' not in query

def valid_subsequent_calls(query):
    return 'likes' in query or 'dislikes' in query 


def get_likes_recs(query, n):
    msg = 'itemSim query: ' + ','.join(query['likes'])
    app.logger.info(msg)
    if n == 0:
        return []
    rec_brandcat = []
    rec_pid = []
    rec_brandcat = get_recs_from_pq(query)
 
    if rec_brandcat:
        rec_pid = brandcat_list2pid_list(rec_brandcat, query, n)

    return rec_pid

def get_dislikes_rec(query, n):
    r = redis.StrictRedis(host = "localhost")
    if n == 0:
        return []
    msg = 'hit random brandcat recs'
    app.logger.info(msg)
    r = redis.StrictRedis(host = "localhost")
    candidate_brandcat = r.sdiff("SS:Recommendations:brandcatlist:cat" + query['cat'],"SS:Recommendations:UID:"+query['uid']+":dislikes") # filter dislikes
    rec_brandcat = random.sample(candidate_brandcat,n)
    rec_pid = brandcat_list2pid_list(rec_brandcat, query, n)
    return rec_pid

def get_recs_from_pq(query):
    r = redis.StrictRedis(host = "localhost")
    pq_key = "SS:Recommendations:UID:"+query['uid']+":prefs:cat" + query['cat']
    if not [i for i in r.keys() if i.startswith('SS:Recommendations:UID:' + query['uid'] + ':prefs:cat' + query['cat'])]:
        rec_brandcat = quick_match(query)
        app.logger.info('return quick match to ' + ','.join(query['likes']))
    else:
        # get 30% from likes, 70% from like_sim
        nLikes2sample = int(len(query['likes'])*0.1)
        nLikes_sim2sample = len(query['likes']) - nLikes2sample 
        rec_likes = []
        rec_likes_sim = []
        if nLikes2sample > 0:
            rec_likes = list( r.srandmember(pq_key+":likes",nLikes2sample))
        if nLikes_sim2sample > 0:
            rec_likes_sim = list(r.srandmember(pq_key+":likes_sim",nLikes_sim2sample))
        rec_brandcat = rec_likes + rec_likes_sim
        print rec_brandcat
        app.logger.info('get ' + str(len(rec_brandcat)) + ' rec_brandcat using ' + str(nLikes2sample) + ' from likes bucket and ' + str(nLikes_sim2sample) + ' from likes_sim bucket')

    return rec_brandcat

def brandcat_list2pid_list(rec_brandcat,query, n):
    rec_pid = []
    for i in rec_brandcat:
        if i:
            if cat_or_brand_invalid(i):
                continue
            app.logger.info('grab recs for ' + i)
            pids = []   
            offset = 0
            d = len(pids) < 1
            while len(pids) < 1:
                tp = brandcat2pid(i, 25, offset)
                tp = filter_shown(tp, query['uid'])
                if not tp:
                    app.logger.info('no more recommendations for ' + i)
                    break
                pids = pids + tp
                offset = offset + 25 # add 25 not length of tp because tp is filtered
                print i, len(pids)
            if pids:
                rec_pid = rec_pid + random.sample(pids,1)
    return rec_pid

def brandcat2pid(brandcat, n, offset):
    pids = []
    query_string = 'http://api.shopstyle.com/api/v2/products?pid=shopsense_app&'+brandcat+'&sort=Popular&limit=' + str(n) + '&offset=' + str(offset)
    p = urllib2.urlopen(query_string).read()
    p = json.loads(p)
    for i in p['products']:
        pids.append(str(i['id']))
    return pids

def cat_or_brand_invalid(i):
    invalid_cat = set([1701,1700,1660,1659,1658,1657,1656,1655,1654,1653,1652,1651,1650,1649,1648,1647,1646,1645,1644,1643,1642,1641,1640,1639,1638,1637,1636,1634,1633,1632,1629,1209,1196,1189,1165,1163,1162,1161,1160,1159,1158,1157,1156,1155,1154,1153,1152,1151,1150,1136,1121,1113,1098,1097,1096,1095,1094,1093,1092,1091,1090,1089,1088,1087,1086,1085,1084,1083,1082,1081,1080,1079,1078,1077,1076,1075,1074,1073,1072,1071,1070,1069,1068,1067,1065,1064,1063,1062,1061,1060,1059,1058,1057,1056,1055,1054,1052,1051,1050,1049,1048,1047,1043,1040,1030,1026,1009,843,834,833,828,815,810,804,803,802,801,800,799,797,796,795,794,789,787,786,784,780,779,778,775,774,773,772,771,770,769,768,755,754,740,735,734,732,729,728,723,722,720,718,716,712710,708,705,703,698,694,692,691,689,687,685,683,679,678,676,675,669,665,664,663,662,661,660,659,658,656,655,653,652,651,649,647,646,645,644,643,642,641,640,639,638,636,632,631,630,629,628,625,624,620,619,618,609,608,607,606,605,604,601,595,594,593,592,590,581,577,572,567,566,564,563,562,561,559,558,557,556,554,550,547,543,542,541,539,538,537,536,535,531,530,529,528,527,526,525,524,523,522,521,519,518,517,516,515,513,512,511,510,509,508,507,506,505,504,503,502,501,499,498,497,496,495,494,493,492,491,490,489,488,487,486,485,484,483,482,481,480,479,478,449,448,447,446,445,444,443,442,441,440,439,438,437,436,435,434,433,432,431,430,429,428,427,426,425,424,423,422,421,415,412,411,410,409,408,407,406,405,404,403,402,401,400,399,398,397,396,395,394,393,392,391,390,389,388,387,386,385,384,383,382,381,380,379,378,377,376,375,374,373,372,371,370,369,368,367,366,365,364,363,362,361,360,359,358,357,356,355,354,353,352,351,350,349,348,347,346,345,344,343,342,341,340,339,338,337,336,335,334,333,332,331,330,329,328,327,326,325,324,322,321,320,319,318,317,316,315,314,312,310,309,308,307,306,305,304,302,301,299,298,297,296,295,294,293,292,289,288,287,286,283,282,281,280,279,278,277,276,275,274,273,272,271,270,269,268,267,266,265,264,262,260,259,258,257,256,255,254,253,252,251,250,248,237,235,234,203,181,161,158,124,104,101,85,55,43,37,32,27,17,6,5])
    invalid_brand = set([10,28,32,37,50,53,88,94,105,125,157,158,159,160,162,186,187,196,213,233,274,293,311,341,342,365,367,368,370,377,389,392,402,406,416,425,428,430,447,479,511,521,545,554,586,597,598,599,605,618,639,642,657,669,673,718,721,722,729,748,754,771,778,798,805,823,829,836,843,869,871,873,874,887,905,908,917,924,939,975,976,992,1023,1025,1027,1029,1042,1047,1049,1070,1071,1073,1080,1081,1083,1091,1111,1125,1144,1152,1157,1158,1162,1163,1173,1178,1186,1189,1209,1231,1232,1251,1253,1307,1324,1359,1375,1376,1405,1406,1407,1413,1428,1429,1434,1444,1458,1459,1471,1475,1486,1519,1539,1549,1562,1567,1585,1594,1607,1608,1631,1641,1644,1645,1655,1659,1663,1690,1708,1713,1736,1737,1750,1754,1759,1765,1766,1776,1782,1788,1801,1808,1836,1840,1852,1859,1864,1876,1877,1952,1973,1979,1988,2017,2029,2030,2042,2063,2082,2099,2127,2131,2134,2137,2138,2143,2181,2182,2196,2197,2198,2200,2220,2261,2287,2291,2306,2330,2350,2356,2420,2431,2444,2450,2477,2482,2485,2503,2552,2556,2562,2567,2602,2605,2620,2648,2681,2695,2746,2804,2830,2853,2889,2899,2904,2963,2974,3004,3013,3047,3048,3053,3078,3080,3104,3106,3109,3190,3199,3201,3203,3205,3209,3217,3218,3222,3253,3264,3280,3301,3331,3333,3336,3369,3382,3393,3426,3443,3452,3467,3497,3507,3551,3631,3656,3658,3664,3755,3819,3841,3866,3910,3918,3919,3923,3928,3935,3939,3955,3985,4013,4065,4096,4119,4172,4180,41824188,4199,4235,4272,4280,4294,4301,4388,4407,4427,4428,4521,4674,4683,4692,4696,4799,4802,5000,5057,5124,5186,5217,5222,5230,5235,5251,5364,5405,5496,5570,5592,5662,5718,5740,5751,5768,5792,5871,5923,5959,5964,5969,6001,60156117,6185,6239,6270,6282,6286,6458,6536,6539,6570,6593,6625,6691,6808,6826,6911,6938,6976,7018,7170,7468,7470,7517,7552,7553,7569,7596,7605,7665,7689,7810,7825,7831,7836,7873,7874,7910,7936,7993,8053,8107,8134,8140,8142,83388384,8462,11794,11795,11828,11834,11838,11853,11915,11948,11957,12057,12132,12139,12153,12168,12348,12357,12379,12416,12434,12446,12530,12662,12665,12729,12788,12843,12971,12973,12976,12990,12993,13113,13174,13200,13252,13499,13524,13551,13572,13618,13625,13630,13636,13670,13681,13690,13720,13806,13864,13870,13925,14103,14106,14111,14533,14650,14672,14823,14874,15329,15351,15353,15406,15477,15689,16299,16332,16915,16966,17190,17358,17600,17644,17702,17706,17808,17865,17962,18071,18175,18181,18296,18388,18428,18429,18438,18454,18478,19552,19587,19732,19740,19852,19952,20053,20093,20116,20154,20176,20180,20206,20231,20247,20347,20366,20461,20609,20615,20660,20973,21239,21257,21310,21389,21390,21564,21575,22473,22594,22725,22955,22960,23063,23115,23328,23498,24710,24957,25393,26368,29024,29348,29472,29640,30455,30506,30507,30509,30511,30512,30513,30880,30881])
    cat, brand = i.split('&')
    cat = int(cat.strip('cat='))
    brand = int(brand.strip('fl=b'))
    if brand in invalid_brand:
        return True
    if cat in invalid_cat:
        return True
    return False

def filter_shown(pids, uid):
    if not pids:
        return []
    r = redis.StrictRedis(host = "localhost")
    pids_filtered = set(pids).difference(set(r.smembers("SS:Recommendations:UID:" + uid + ":shown")))
    return list(pids_filtered)

def is_new_visitor(query):
    msg = 'check user'
    app.logger.info(msg)
    UID = query['uid']    
    r = redis.StrictRedis(host = "localhost")
    msg = 'redis uid key is ' + "SS:Recommendations:UID:"+UID
    app.logger.info(msg)
    if not [i for i in r.keys() if i.startswith("SS:Recommendations:UID:" + UID + ":prefs:cat" + query['cat'] )]:
        flag = 1
        msg = 'brand new'
        app.logger.info(msg)
        quiz = r.hgetall('SS:Recommendations:quiz:cat' + query['cat']).keys()
        return 1, quiz
    else:
        flag = 0
        msg = 'seen ya'
        app.logger.info(msg)
        return 0, []

def quick_match(query):
    r = redis.StrictRedis(host = "localhost")
    likes = query['likes']
    brandcat = []
    if likes:
        for i in likes:
            brandcat.append(r.hget('SS:Recommendations:quiz:cat' + query['cat'], i))
    # prefs in queue
    return brandcat        

def get_random(query, n):
    r = redis.StrictRedis(host = "localhost")
    if n == 0:
        return []
    msg = 'hit random'
    app.logger.info(msg)
    r = redis.StrictRedis(host = "localhost")
    if query['cat'] not in ('109', '219'):
        query['cat'] = '109'
    notshown =  r.sdiff("SS:Recommendations:prodlist:cat"+query['cat'],"SS:Recommendations:UID:"+query['uid']+"shown")
    recs = random.sample(notshown,n)
    return recs

@async
def deposit_user_prefs(query):
    pq_key = "SS:Recommendations:UID:"+query['uid']+":prefs:cat" + query['cat']
    r = redis.StrictRedis(host = "localhost")
    if query['likes']:
        for i in query['likes']:
            out = pid2cat.pid2cat(i,3)
            if out[0] and out[1]:
                brandcat = 'cat=' + str(out[1]) + '&fl=b' + str(out[0])
                brandcat_sim = get_sim(brandcat, query)
                app.logger.info('registering ' + brandcat + ' as likes and ' + str(len(brandcat_sim)) + ' likes_sim')
                r.sadd(pq_key + ":likes",brandcat)
                for j in brandcat_sim:
                    r.sadd(pq_key + ":likes_sim",j)

    if query['dislikes']:
        for i in query['dislikes']:
            out = pid2cat.pid2cat(i,3)
            if out[0] and out[1]:
                brandcat = 'cat=' + str(out[1]) + '&fl=b' + str(out[0])
                brandcat_sim = get_sim(brandcat, query)
                app.logger.info('registering ' + brandcat + 'as dislikes and ' + str(len(brandcat_sim)) + ' dislikes_sim')
                r.sadd(pq_key + ":dislikes",brandcat)
                for j in brandcat_sim:
                    r.sadd(pq_key + ":dislikes_sim",j)
    app.logger.info('user preference registered')

def get_sim(brandcat, query):
    client = predictionio.Client("c9E6ehRTIIRUs3ZLgqXjsXWyr6CSGKGiHRoWINzk2POfzcjWSyeoglEeQVmWYfkK",apiurl ='http://sugarqapio02.sugarops.com:8000') # stable uid-brandcat
    brandcat_sim = []
    try:
        if query['cat'] == '109':
            rec =  client.get_itemsim_topn("women-shoes",brandcat , 5 , {"pio_itypes":("cat109",)})   
        elif query['cat'] == '219':
            rec =  client.get_itemsim_topn("men-shoes",brandcat , 5 , {"pio_itypes":("cat219",)})   
    except:
        rec = []
    if rec:
        brandcat_sim = rec['pio_iids']
    return brandcat_sim


@app.route('/recommendation/api/v1.0/products', methods = ['GET','POST'])
def get_recommendation(query_string=[]):

    r = redis.StrictRedis(host = "localhost")
    if query_string:
        query = process_query_string(query_string)
        internal = 1
    else:
        query = process_request(request)
        internal = 0
    query = check_valid_query(query)
    query = set_limit(query)
    msg = 'query received: ' + str(query)
    app.logger.info(msg)

    is_new, recommendations = is_new_visitor(query)
    
    if is_new:
        msg = 'new user, giving quiz'
        app.logger.info(msg)
        pass
    elif valid_subsequent_calls(query):

        nLikes = len(query['likes'])
        nDislikes = len(query['dislikes'])
        nAll = nLikes + nDislikes 
        if nAll > 1:
            ratio = nLikes/float(nAll)
            # hit like and dislike at least once 
            nLikes = 1 + int(round((nAll-2)*ratio))
            nDislikes = nAll - nLikes
        msg = 'return user, provide recs based on inferred limit'
        app.logger.info(msg)
        rec_likes = get_likes_recs(query, nLikes)
        msg =  'obtained recs for ' + str(nLikes) + ' likes: ' + ','.join(rec_likes)
        app.logger.info(msg)
        rec_dislikes = get_dislikes_rec(query, nDislikes)
        msg = 'obtained recs for '  + str(nDislikes) + ' dislikes: ' + ','.join(rec_dislikes)
        app.logger.info(msg)
        recommendations = rec_likes + rec_dislikes # override the empty recs
    else: 
        msg = 'query not initial or subsequent, show random then'
        app.logger.info(msg)
        recommendations = []

    msg = 'current recommendations before final trims: ' + ','.join(recommendations)
    app.logger.info(msg)

    # fall back recommendations:
    short = query['limit'] - len(recommendations)
    while short > 0: 
        msg = 'recommendations are ' + str(short) + ' short. get random recs'
        app.logger.info(msg)
        rec = get_random(query, short)
        recommendations = recommendations + rec
        short = query['limit'] - len(recommendations)
    if short < 0:
        msg = 'rec is ' + str(-1*short) + ' more than needed. trim down'
        app.logger.info(msg)
        recommendations = recommendations[:short]

    # register user preferences
    deposit_user_prefs(query)
    app.logger.info('finished with this call')

    for rec in recommendations:
        success = r.sadd("SS:Recommendations:UID:"+ query['uid'] + ":shown", rec)
        if is_new:
            r.expire("SS:Recommendations:UID:"+ query['uid'] + ":shown", 15*60) # expires in 15 minutes

    # registere the recommendation before returning response
    if internal:
        return { 'pid': recommendations }
    else:
        return jsonify( { 'pid': recommendations } ) ,201, \
    {'Access-Control-Allow-Origin': '*'} 

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify( { 'error': 'Not found' } ), 404) 

@app.errorhandler(400)
def not_found(error):
    return make_response(jsonify( { 'error': 'Bad Request. Revise Parameters.' } ), 400)