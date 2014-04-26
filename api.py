from flask import Flask, jsonify, abort, request, make_response, url_for
from app import app
import predictionio
import redis
import random

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
    r = redis.StrictRedis(host = "localhost")
    if n == 0:
        return []
    #client = predictionio.Client(\
    #"3RtGLR3kYUc3MF3Y0hP7lIS5TVmxeyigOhu69QqN6XxRTKqdLmB69oKcsjeydiQg",\
    #apiurl='http://ec2-54-196-218-123.compute-1.amazonaws.com:8000')
    #client = predictionio.Client(\
    #"PjzXdALeZVsZFDPFWa0DdU6bGQtAklUwRI3KHSNlfM21JXrT2cjCy6sh66g0hULQ",\
    #apiurl='http://sugarqapio01.sugarops.com:8000')
    client = predictionio.Client("3lGQW7bsXs3EdhY8tTCJxpKOIlQIzH7JyDx7LQbpJCzfG8fHZxEIdwnolpySrtI0",apiurl='http://ec2-23-22-235-230.compute-1.amazonaws.com:8000')
    recommendations = []
    msg = 'itemSim query: ' + ','.join(query['likes'])
    app.logger.info(msg)
    for idx,i in enumerate(query['likes']):
        try:
            if query['cat'] == '109':
                rec =  client.get_itemsim_topn("women-shoes",\
            i , 50 , {"pio_itypes":("cat109",)})   
            elif query['cat'] == '219':
                rec =  client.get_itemsim_topn("men-shoes",\
            i , 50 , {"pio_itypes":("cat219",)})   
            else:
                rec = []
        except:
            rec = []
        if rec:
            recommendations = recommendations + rec['pio_iids']
    recommendations_norepeat = set(recommendations).difference(set(r.smembers("SS:Recommendations:UID:" + query['uid'])))
    app.logger.info('n is ' + str(n) + ' rec_norepeat is ' + str(len(recommendations_norepeat)) )
    if len(recommendations_norepeat) > n:
        recommendations_norepeat = random.sample(recommendations_norepeat,n)
    return recommendations_norepeat

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



# def get_random_rank(query, n):
#     if n == 0:
#         return []
#     print 'hit randomRank'
#     client = predictionio.Client(\
#     "3lGQW7bsXs3EdhY8tTCJxpKOIlQIzH7JyDx7LQbpJCzfG8fHZxEIdwnolpySrtI0",\
#     apiurl='http://ec2-23-22-235-230.compute-1.amazonaws.com:8000')
#     UIDs = [2751196, 1435256, 7843067, 3643588,1435256,7843067,3643588,\
#     2751196,23317956,8797292,1738082,3273584,4027617,23130354,9203125,\
#     4092171,22569919,2023673,1600038,9027903,22485718,6382491,2536719,8455206]
#     UID_sampled = random.sample(UIDs,5)
#     rec = []
#     for UID in UID_sampled:
#         client.identify(UID)
#         rec_tp = client.get_itemrec_topn("randomRank",50, {"pio_itypes":("Shoes",)})
#         rec_tp = rec_tp['pio_iids']
#         rec = rec + rec_tp
#     print rec
#     recommendations = random.sample(rec, min(n,len(rec)))
#     return recommendations

# def get_latest_rank(query):
#     client = predictionio.Client(\
#     "3lGQW7bsXs3EdhY8tTCJxpKOIlQIzH7JyDx7LQbpJCzfG8fHZxEIdwnolpySrtI0",\
#     apiurl='http://ec2-23-22-235-230.compute-1.amazonaws.com:8000')
#     UID = 1435256 # to implement later
#     client.identify(UID)
#     recommendations = client.get_itemrec_topn("latestRank",query['limit'])
#     recommendations = recommendations['pio_iids']
#     return recommendations

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
        # quiz = [35554451, 289569549, 381653074, 251492272, 277627106, 381653074\
        # , 364057509, 14286641, 362024011, 363709083, 372945408, 343527540, \
        # 289954679, 361265129, 327482514, 351551809, 262960565, 287513377, \
        # 321230959, 380815266, 265006493, 377188583,340846376, 251379999, \
        # 405532523 ]
        # recommendations = quiz[:query['limit']]
        quiz = get_random(query, query['limit'])
        return flag, quiz
    else:
        flag = 0
        msg = 'seen ya'
        app.logger.info(msg)
        return 0, []

# shown = r.hget('SS:Recommendations', UID)
# print 'shown', shown
# for idx, i in enumerate(quiz):
#     if str(i) not in shown:
#         print 'quiz number', idx
#         recommendations = i
#         r.hset('SS:Recommendations', UID, shown+','+str(i))
#         print r.hget('SS:Recommendations', UID)
#         #flag = 1 
#         break
# else:
#     recommendations = []
    

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
