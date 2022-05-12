from os import listdir, path
from os.path import isfile, join
import csv
import sys
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build 
from googleapiclient.http import BatchHttpRequest
import httplib2
import json

SCOPES = ['https://www.googleapis.com/auth/indexing']
ENDPOINT = 'https://indexing.googleapis.com/v3/urlNotifications:publish'

print(' ')

print('--[Onwise Instant Indexer]--')

def instructions():
    print('Plaats de .csv en .json bestanden in de "index" map die in dezelfde map staat als instant-indexer.exe,')
    print('allebei met dezelfde naam.')
    print('Bijv. onwise.csv, onwise.json.')
    print(' ')
    print('Het .csv bestand heeft twee kolommen, eerst "url", dan "type".')
    print('Plaats de URLs die je wil indexeren of verwijderen in de "url" kolom.')
    print('Plaats "URL_UPDATED" of "URL_DELETED" in de "type" kolom, liggend aan wat je wil doen met de URL.')
    print(' ')
    print('Het .json bestand moet de API key file zijn van Google Cloud Platform.')
    print('Volg hier stap 2 tot en met stap 4.3: https://rankmath.com/blog/google-indexing-api/#create-indexing-api-project')
    print('en hernoem het bestand naar dezelfde naam als het .csv bestand.')
    print(' ')
    print('Start dit programma daarna opnieuw op.')
    print(' ')
    input('Druk op enter om de instructies te sluiten')
    sys.exit()

if path.exists('index'):
    index_files = [f for f in listdir('index') if isfile(join('index', f))]
    
    if not index_files:
        print(' ')
        print('Geen index bestanden gevonden')
        print(' ')
        print('---')
        print(' ')
        instructions()
else:
    print(' ')
    print('"index" map niet gevonden, wordt nu aangemaakt...')
    os.mkdir('index')
    print(' ')
    print('---')
    print(' ')
    instructions()


print(' ')

for file in index_files:
    if file.rsplit('.')[1] != 'csv':
        continue

    print('Indexatie voor "'+file.rsplit('.')[0]+'" begint!')
    print(' ')

    JSON_KEY_FILE = 'index/'+file.rsplit('.')[0]+'.json'

    credentials = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE, scopes=SCOPES)
    http = credentials.authorize(httplib2.Http())

    with open('index/'+file, 'r') as index_csv:
        index_object = csv.DictReader(index_csv)

        # Build service
        service = build('indexing', 'v3', credentials=credentials)
 
        def insert_event(request_id, response, exception):
            if exception is not None:
                print(exception)
            else:
                try:
                    print('['+response['urlNotificationMetadata']['latestUpdate']['type']+'] '+response['urlNotificationMetadata']['url'])
                except:
                    print('['+response['urlNotificationMetadata']['latestRemove']['type']+'] '+response['urlNotificationMetadata']['url'])

        batch = service.new_batch_http_request(callback=insert_event)

        for row in index_object:
            batch.add(service.urlNotifications().publish(body=row))

        batch.execute()
    print(' ')
    print('---')
    print(' ')

print('Indexatie compleet!')
print(' ')
print('---')
print(' ')
input('Druk op enter om het programma te sluiten...')