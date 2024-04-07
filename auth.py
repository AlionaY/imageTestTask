from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials


def authenticate_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    spreadsheet_service = build('sheets', 'v4', credentials=credentials)
    return spreadsheet_service
