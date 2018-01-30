import csv

import requests
from pprint import pprint
import psycopg2
import json
from psycopg2.extensions import AsIs
from datetime import datetime
import time
import math
import re

conn = psycopg2.connect(database='')
conn.autocommit = True
cur = conn.cursor()

table = 'youtube_music'
cur.execute('create table if not exists %s (channel text, id text, title text primary key, videos json, artist text)', [AsIs(table)])
# # Amoeba, Pitchfork, NPR Music, Boiler Room, The FADER, NardwuarServiette, David Dean Burkhart
channels = {'Amoeba': 'UC9DkCKm4_VDztRRyge4mCJQ', 'Pitchfork': 'UC7kI8WjpCfFoMSNDuRh_4lA',
            'NPR Music': 'UC4eYXhJI4-7wSWc8UNRwD4A',
            'Boiler Room': 'UCGBpxWJr9FNOcFYA5GkKrMg', 'The FADER': 'UCRCOCvfOkoqneyQCbNOUPwg',
            'NardwuarServiette': 'UC8h8NJG9gacZ5lAJJvhD0fQ',
            'David Dean Burkhart': 'UCNYJOAz1J80HEJy2HSM772Q', 'KEXP': 'UC3I2GFN_F8WudD_2jUZbojA',
            'Audiotreetv': 'UCWjmAUHmajb1-eo5WKk_22A', 'The A.V. Club': 'UCsDdQUPa4NvPvf2f00E5zfw'}
api_key = ''
max_results = 50

for user_name, user_id in channels.items():
    print('getting channel %s with user id %s' % (user_name, user_id))
    r = requests.get('https://www.googleapis.com/youtube/v3/channels?part=contentDetails&id=%s&key=%s' % (user_id, api_key))
    uploads_id = r.json()['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    print('uploads id:', uploads_id)
    page_token = ''
    page = 1
    total_items = 0
    total_videos = 0
    total_pages = 0
    duplicates = 0
    while 1:
        r = requests.get('https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&maxResults=50&playlistId=%s&key=%s&pageToken=%s' % (uploads_id, api_key, page_token))
        j = r.json()
        if page == 1:
            page_info = j.get('pageInfo')
            if page_info:
                total_videos = page_info['totalResults']
                print('total videos uploaded %d' % total_videos)
                total_pages = math.ceil(round(total_videos/max_results))
        print('%s: %d/%d' % (user_name, page, total_pages))
        page_token = j.get('nextPageToken')
        page_items = j.get('items')
        if page_items:
            for item in page_items:
                try:
                    cur.execute('insert into %s (channel, id, title, videos) values (%s, %s, %s, %s)',
                                [AsIs(table), user_name, user_id, item['snippet']['title'], json.dumps(item)])
                except psycopg2.IntegrityError:
                    # duplicates += 1
                    # if duplicates > 100:
                    #     raise SystemExit('too many duplicates')
                    pass
        if not page_token:
            print('finished %s' % user_name)
            break
        page += 1
        time.sleep(1)


av_re = re.compile(r'(.*) performs')
at_re = re.compile(r'(.*) - .* - Audiotree Live')
kexp_re = re.compile(r'(.*) - .*(Live on KEXP)')
am_re = re.compile(r'(.*) - (.*|What\'s In My Bag\?)(Live at Amoeba)?')
npr_re = re.compile(r'(.*)\: NPR Music Tiny Desk Concert')
ns_re = re.compile(r'Nardwuar vs. (.*)')
ns_sub_re = re.compile(r'( ?\(\d{4}\))?(pt \d+.*)?(: .*)?( - the extended version)?( \/.*)?', re.IGNORECASE)
db_re = re.compile(r'(.*) - >*')
#
cur.execute("select title, channel from %s", [AsIs(table)])
for title, channel in cur.fetchall():
    use_re = None
    if channel == 'KEXP':
        use_re = kexp_re
    elif channel == 'Audiotreetv':
        use_re = at_re
    elif channel == 'The A.V. Club':
        use_re = av_re
    elif channel == 'Amoeba':
        use_re = am_re
    elif channel == 'NPR Music':
        use_re = npr_re
    elif channel == 'NardwuarServiette':
        m = ns_re.search(title)
        if m and m.group(1):
            cur.execute("update %s set artist = %s where title = %s and channel = %s", [AsIs(table), ns_sub_re.sub('', m.group(1)).strip(), title, channel])
        continue
    elif channel == 'David Dean Burkhart':
        use_re = db_re
    if use_re:
        m = use_re.search(title)
        if m and m.group(1):
            cur.execute("update %s set artist = %s where title = %s and channel = %s", [AsIs(table), m.group(1).strip(), title, channel])


'''
select artists from (select distinct(artist) as artists from youtube_music where channel = 'KEXP'
  and artist in (select distinct(artist) from youtube_music where channel = 'Audiotreetv' and artist is not null)) foo
  where artists in (select distinct(artist) from youtube_music where channel = 'The A.V. Club');
'''

'''
create table youtube_flat (audiotreetv text, boiler_room text, amoeba text, the_fader text, david_dean_burkhart text,
the_av_club text, kexp text, npr_music text, nardwuarserviette text);
'''

# for channel, column in [('Audiotreetv', 'audiotreetv'), ('Boiler Room', 'boiler_room'), ('Amoeba', 'amoeba'),
#                         ('The FADER', 'the_fader'), ('David Dean Burkhart', 'david_dean_burkhart'), ('The A.V. Club', 'the_av_club'),
#                         ('KEXP', 'kexp'), ('NPR Music', 'npr_music'), ('NardwuarServiette', 'nardwuarserviette')]:
#     print("insert into youtube_flat (%s) select artist from youtube_music where channel = '%s' and artist is not null order by artist;" % (column, channel))
# cur.execute("insert into youtube_flat (%s) select artist from youtube_music where channel = %s and artist is not null order by artist", [AsIs


cur.execute('select channel, artist from youtube_music where artist is not null')
rows = {}
for channel, artist in cur.fetchall():
    if channel not in rows:
        rows[channel] = [artist]
    else:
        rows[channel].append(artist)

rows = {k: sorted(v) for k, v in rows.items()}
all_artists = {artist for artists in rows.values() for artist in artists}
#
headers = ['Amoeba', 'Audiotreetv', 'David Dean Burkhart', 'KEXP', 'NPR Music',
           'NardwuarServiette', 'The A.V. Club']
with open('youtube_flat.csv', 'w') as wfile:
    wcsv = csv.writer(wfile)
    wcsv.writerow(headers)
    csv_len = max([len(v) for v in rows.values()])
    print(csv_len)
    for idx in range(csv_len):
        row = []
        for channel in headers:
            try:
                row.append(rows[channel][idx])
            except IndexError:
                row.append('')
        wcsv.writerow(row)

# crawler=# create table youtbue_artist_count as select artist, count(*) from youtube_music where artist is not null group by 1 order by 2 desc;

cur.execute('select artist, count(*) from youtube_music where artist is not null group by 1 order by 2 desc')
counts = {t[0]: t[1] for t in cur.fetchall()}
multi_counts = {}

for a in all_artists:
    multi = 0
    for c in headers:
        if a in rows[c]:
            multi += 1
    multi_counts[a] = multi

print(sorted(multi_counts.items(), key=lambda x: x[1], reverse=True))

new_counts = {k: multi_counts[k]*v for k, v in counts.items()}

print(sorted(new_counts.items(), key=lambda x: x[1], reverse=True))

with open('youtube_artist_scores.csv', 'w') as wfile:
    wcsv = csv.writer(wfile)
    wcsv.writerow(['artist', 'score', 'weighted_score'])

    for a in all_artists:
        wcsv.writerow([a, counts[a], new_counts[a]])