from lib.dataSc import load_data

def lambda_handler(evnt, context):
    response = load_data(context.story_id)
