from lib.db import db_connection, fetch_postgres_data, db_cleansed_data_db, save_clean_data, fetch_clean_data
import pandas as pd
import sys

def load_data(id):
    '''
        id: id of the story where the data is stored
    '''
    sys.stdout.write("Fetching data")
    connection = db_cleansed_data_db()
    clean_data_stored = fetch_clean_data(id)
    if clean_data_stored:
        sys.stdout.write("There is an existing report saved, loading from state")
        return { data: pd.DataFrame(clean_data_stored["answers"]) }
    connection = db_connection()
    sys.stdout.write("Gatherin story data")
    
    # Build the queries
    story_query = '''SELECT 
                        s.id story_id,
                        s.name story_name,
                        s.slug,
                        s.description,
                        s.settings,
                        s.start_text,
                        s.token,
                        s.intl_provider,
                        s.company_id,
                        s.ending_url,
                        s.publish_date,
                        s.languages_configuration,
                        s.ofline,
                        s.show_results,
                        s.show_on_home_page,
                        s.aditional_player,
                        s.more_information,
                        s.unpublish_date,
                        s.header_description,
                        s.authentication,
                        st.id translation_id,
                        st.name story_name,
                        st.title story_title,
                        st.description,
                        st.start_text
                    FROM stories s
                    INNER JOIN story_translations st ON st.story_id = s.id
                    WHERE s.id = {story_id} AND st.locale = ''' + "'en'"
    story_query  = parse_queries(story_query, 'en', '{story_id}', id)
    
    storydata = fetch_postgres_data(connection, story_query)
    
    sys.stdout.write("Gatherin slides data")
    

    slides_query = '''SELECT 
                    s.id slide_id,
                    q.id question_id,
                    s.position,
                    s.story_id,
                    s.content_type,
                    q.show_label,
                    q.allow_skip,
                    q.instructions,
                    q.number_answers,
                    q.question_type_id,
                    q.position question_position,
                    q.composed_question_id,
                    q.logic,
                    qt.id question_translation_id,
                    qt.name question_name,
                    qt.instructions question_instructions,
                    qt.description
                FROM slides s
                LEFT JOIN questions q ON q.id = s.content_id
                LEFT JOIN question_translations qt ON qt.question_id = q.id
                WHERE s.story_id = {story_id} AND ''' + "qt.locale = 'en' ORDER BY s.position ASC"
    slides_query  = parse_queries(slides_query, 'en', '{story_id}', id)
    
    slides_data = fetch_postgres_data(connection, slides_query)
    
    sys.stdout.write("Gatherin choices data")
    
    choices_query = '''SELECT
                        ch.id choice_id,
                        ch.question_id,
                        ch.position choice_position,
                        ch.pointer,
                        ch.is_other_choice,
                        ch.none_of_the_above,
                        ch.skip_choice,
                        ct.id choice_translation_id,
                        ct.value
                    FROM choices ch 
                    LEFT JOIN choice_translations ct ON ct.choice_id = ch.id
                    WHERE ch.question_id IN (SELECT q.id FROM slides s
                    INNER JOIN questions q ON q.id = s.content_id
                    WHERE s.story_id = {story_id}) ''' + "AND ct.locale = 'en'"
    choices_query = parse_queries(choices_query, 'en', '{story_id}', id)
    slides_data = fetch_postgres_data(connection, choices_query)
    
    conn.close()
    sys.stdout.write("Closing connection and cleaning data")
    # save parsed data 
    parsed_data = data_cleaning(response)
    sys.stdout.write("Saving clean data")
    save_clean_data(connection, { "storyId": id, "answers": parsed_data.to_json("records")})
    return { data: df }
    
def parse_queries(query='', locale='en', key_to_replace='', value_to_replace=''):
    return query.replace(key_to_replace, "'{}'".format(value_to_replace))

def data_cleaning(db_response):
    '''
        db_response: db array data response
        returns: Data cleaning and parsing
    '''
    tmp = []
    # data parsing
    for el in db_response:
        el["question"] = el['_source']['question']
        el["position"] = el['_source']['position']
        el["category"] = el['_source']['category']
        el["choice"] = el['_source']['choice']
        el["viewing"] = el['_source']['viewing']
        el["selected"] = el['_source']['selected']
        tmp.append(el)
    # pd.dropna()
    df = pd.DataFrame(tmp)

    return df

    