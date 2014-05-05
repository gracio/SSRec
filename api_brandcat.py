from flask import Flask, jsonify, abort, request, make_response, url_for
from app import app
import predictionio
import redis
import random
import pid2cat
import urllib2
import json
from decorators import async


from datetime import timedelta
from flask import current_app
from functools import update_wrapper

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


# def get_item_rec(query):
#     client = predictionio.Client(\
#     "3WEome2D7A7IrVpvU3cuOqw8ItQi4Rnw6sfmSxF84DDllwzrDsBQTnn9Jn3JbrT7",\
#     apiurl='http://ec2-54-226-118-108.compute-1.amazonaws.com:8000')
#     client.identify(query['uid'])
#     try:
#         recommendations = client.get_itemrec_topn("itemRec",query['limit'])
#     except:
#         recommendations = []
#     if recommendations:
#         recommendations = recommendations['pio_iids']
#     return recommendations

def get_item_sim(query, n):
    if n == 0:
        return []
    client = predictionio.Client("c9E6ehRTIIRUs3ZLgqXjsXWyr6CSGKGiHRoWINzk2POfzcjWSyeoglEeQVmWYfkK",apiurl ='http://sugarqapio02.sugarops.com:8000') # stable uid-brandcat
    rec_brandcat = []
    rec_pid = []
    msg = 'itemSim query: ' + ','.join(query['likes'])
    app.logger.info(msg)
    for idx,i in enumerate(query['likes']):
        rec = []
        out = pid2cat.pid2cat(i,3)
        if out[0] and out[1]:
            brandcat = 'cat=' + str(out[1]) + '&fl=b' + str(out[0])
            try:
                if query['cat'] == '109':
                    rec =  client.get_itemsim_topn("women-shoes",\
                    brandcat , 5 , {"pio_itypes":("cat109",)})   
                elif query['cat'] == '219':
                    rec =  client.get_itemsim_topn("men-shoes",\
                    brandcat , 5 , {"pio_itypes":("cat219",)})   
            except:
                rec = []
        if rec:
            rec_brandcat = rec_brandcat + rec['pio_iids']
    rec_pid = []
    if rec_brandcat:
        for i in rec_brandcat:
            app.logger.info('grab recs for ' + i)
            pids = []   
            offset = 0
            d = len(pids) < 5
            while len(pids) < 5:
                tp = brandcat2pid(i, 25, offset)
                tp = filter_shown(tp, query['uid'])
                if not tp:
                    app.logger.info('no more recommendations for ' + i)
                    break
                pids = pids + tp
                offset = offset + 25 # add 25 not length of tp because tp is filtered
                print i, len(pids)
            rec_pid = rec_pid + pids
    app.logger.info('sample ' + str(n) + ' from ' + str(len(rec_pid)) + ' rec_pid')
    if len(rec_pid) > n:
        rec_pid = random.sample(rec_pid,n)
    return rec_pid

def get_random(query, n):
    r = redis.StrictRedis(host = "localhost")
    if n == 0:
        return []
    msg = 'hit random'
    app.logger.info(msg)
    r = redis.StrictRedis(host = "localhost")
    if query['cat'] not in ('109', '219'):
        query['cat'] = '109'
    notshown =  r.sdiff("SS:Recommendations:cat"+query['cat'],"SS:Recommendations:UID:"+query['uid'])
    recs = random.sample(notshown,n)
    return recs


def brandcat2pid(brandcat, n, offset):
    pids = []
    query_string = 'http://api.shopstyle.com/api/v2/products?pid=shopsense_app&'+brandcat+'&sort=Popular&limit=' + str(n) + '&offset=' + str(offset)
    p = urllib2.urlopen(query_string).read()
    p = json.loads(p)
    for i in p['products']:
        pids.append(str(i['id']))
    return pids

def filter_shown(pids, uid):
    r = redis.StrictRedis(host = "localhost")
    pids_filtered = set(pids).difference(set(r.smembers("SS:Recommendations:UID:" + uid)))
    return list(pids_filtered)

def is_new_visitor(query):
    msg = 'check user'
    app.logger.info(msg)
    UID = query['uid']    
    r = redis.StrictRedis(host = "localhost")
    msg = 'redis uid key is ' + "SS:Recommendations:UID:"+UID
    app.logger.info(msg)
    if "SS:Recommendations:UID:"+UID not in r.keys():
        flag = 1
        msg = 'brand new'
        app.logger.info(msg)
        quiz = r.hgetall('SS:Recommendations:quiz:cat' + query['cat']).keys()
        # recommendations = quiz[:query['limit']]
        #quiz = get_random(query, query['limit'])
        return flag, quiz
    else:
        flag = 0
        msg = 'seen ya'
        app.logger.info(msg)
        return 0, []

def quick_match(query):
    likes = query['likes']
    return likes        

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
        if nLikes > 0 and nLikes/float(nAll) < 0.7:
            nLikes = int(round( nAll * 0.7))
            nDislikes = int(nAll - nLikes)
        msg = 'return user, provide recs based on inferred limit'
        app.logger.info(msg)
        rec_likes = get_item_sim(query, nLikes)
        msg =  'get recs for ' + str(nLikes) + ' likes: ' + ','.join(rec_likes)
        app.logger.info(msg)
        rec_dislikes = get_random(query, nDislikes)
        msg = 'get recs for '  + str(nDislikes) + ' dislikes: ' + ','.join(rec_dislikes)
        app.logger.info(msg)
        recommendations = rec_likes + rec_dislikes # override the empty recs
    else: 
        msg = 'query not initial or subsequent, show random then'
        app.logger.info(msg)
        recommendations = []

    msg = 'current recommendations: ' + ','.join(recommendations)
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

    for rec in recommendations:
        success = r.sadd("SS:Recommendations:UID:"+ query['uid'], rec)
        if is_new:
            r.expire("SS:Recommendations:UID:"+ query['uid'], 15*60) # expires in 15 minutes

    #show_msg('test')

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

    #task = filter(lambda t: t['id'] == task_id, tasks)

# @app.route('/recommendation/api/v1.0/products/<str:query>', methods = ['POST'])
# def create_task():
#     if not 'uid' in query or not 'cat' in query:
#         abort(400)
#     qterms = query.split('&')
#     qterms = [i.split('=') for i in qterms]

#     query = {
#         'id': tasks[-1]['id'] + 1,
#         'title': request.json['title'],
#         'description': request.json.get('description', ""),
#         'done': False
#     }
#     tasks.append(task)
#     return jsonify( { 'task': task } ), 201