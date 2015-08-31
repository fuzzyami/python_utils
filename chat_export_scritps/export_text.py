#!/usr/bin/python
"""Export text chats from english speaking users with the format requested by tomobox."""
import sys
from set_env import set_env
set_env('prod')
from couchbase.views.iterator import View
from couchbase.views.params import Query
import couchbase
from ricapi.init_db import ReplicaSession as Session
import collections
from guess_language import guessLanguage
from datetime import datetime


TEXT_COUCH = 'ec2-54-158-199-82.compute-1.amazonaws.com'
TEXT_BUCK = 'textchat'
HIST_DESIGN = 'history'
HIST_VIEW = 'history'
MINIMAL_THREAD_LENGTH = 20
OUTPUT_FILE_NAME = 'text_chat_dump.txt'
ISRAEL_COUNTRY_CODE = 107

cb = couchbase.Couchbase.connect(host=TEXT_COUCH, bucket=TEXT_BUCK)
global_dict = {}

def export_texts(start_at, num_users):
    
    """Export text-chats to file."""
    f = open(OUTPUT_FILE_NAME, 'a')
    userids = get_userids(start_at, num_users)
    print('got %s userids', len(userids))

    # get a dictionary of userids (pairs of userids)
    for userid in userids:
        dct = {}
        conversees = get_conversees(userid)
        if len(conversees) > 0 :
            dct[str(userid)] = get_conversees(userid)
	    global_dict.update(dct)

    # for each pair, get the thread and write to file
    for k in global_dict.keys():
        try:
            print 'getting thread for user %s and %s' % (k, global_dict[k])
            for target in global_dict[k]:
                text = get_thread(k, target)
                if text:
                    f.write(text.encode('utf8'))
        except:
            pass
    f.close()
        

def is_automatic_text(text):
    automatic_texts = ['Hey, tried to call you but you didn\'t answer. Call me back when you can.', 
			'Welcome to Rounds']
    for t in automatic_texts:
        if text.find(t) > -1:
            return True
    return False

def get_conversees(userid):
    """Get a list of all the userids that communicated with the given userid.
    """
    q = Query(mapkey_single=str(userid), descending=False)
    docs = [res.doc.value for res in
            View(cb, HIST_DESIGN, HIST_VIEW, query=q, include_docs=True)
            if res.doc.value['type'] == 'message']
    userids = []
    for doc in docs:
        userids.extend(doc['recipients'])
    if userids:
        userids = list(set(userids))
        if str(userid) in userids:
            userids.remove(str(userid))
    return userids


def is_english(thread):
    """Return true if the given string is in English."""
    if guessLanguage(thread) == "en":
        return True
    return False 


def get_userids(start_at, num_users):
    """Selects userids to get text threads for."""
    #query = "select distinct id from users join user_device_details on users.id = user_device_details.rounds_userid where (user_device_details.country_id = 227 or user_device_details. country_id = 225) and users.id > 30000000 limit %s" % NUM_USERIDS
    query = "select distinct id from users join user_device_details on users.id = user_device_details.rounds_userid where user_device_details.country_id != 107 and users.id > %s limit %s" % (start_at, num_users)
    return list(set([r[0] for r in Session.execute(query).fetchall()]))


def unix_time(time):
    """Convert the text-chat timestamp to unix timestamp."""
    dt = (datetime.strptime(time[0:-6],'%Y-%m-%dT%H:%M:%S'))
    return str(int(dt.strftime("%s")))


def get_thread(user_id, user_id2):
    """Get a text-chat thread between the two users.
    
    This function filters out threads that are too short,
    or are not in English. In addition it filters out 
    'automatic texts' that are built into the ROUNDS app.
    The output is returned as a single string, with 
    the format specified by Tomobox.

    If the thread is too short or not in English None is returned.
    """
    q = Query(mapkey_single=str(user_id2), descending=False)
    docs = [res.doc.value for res in
            View(cb, HIST_DESIGN, HIST_VIEW, query=q, include_docs=True)
            if res.doc.value['type'] == 'message']


    # find only docs with the requested user_id:
    docs = [(doc) for doc in docs
            if user_id in (doc['recipients'] + [doc['from']]) ]

    # filter out short threads
    if len(docs) < MINIMAL_THREAD_LENGTH:
        return None

    # convert to a single string
    conversation = ''
    if docs:
        for doc in docs:
 
            if is_automatic_text(doc['content']['body']):
                continue
           
            from_id = doc['from']
            to_id = doc['recipients']
            try: 
                to_id.remove(from_id)
                to_id = to_id[0]
            except:
                continue
            else:
                body = doc['content']['body']
                if body.find('\n') > 0:
                    try:
                        body = body.replace('\n', '')
                    except:
                        pass
                conversation = conversation + unix_time(doc['timestamp']) + ' ' + from_id + ' ' + to_id +  ' ' + body + '\n'

    # filter out non english
    if not is_english(conversation):
        return None

    return conversation

if __name__ == '__main__':
   start_at_user = sys.argv[1]
   num_users = sys.argv[2]
   export_texts(start_at_user, num_users)
