import pandas as pd
from datetime import datetime


def get_portfolio():
    """ Retrieve and pre-process portfolio data.
    
    Pre-processing steps:
        * Offer types are depicted in dummy columns.
        * Offer duration is depicted in hours.
        * `id` column is renamed as `offer_id`.
        * `duration` is renamed as `duration_hs`.
        * Columns are re-ordered.
    
    Return.
        Pandas Dataframe. Pre processed portfolio data.
    
    """
    # read in the json file
    portfolio = pd.read_json('data/portfolio.json', orient='records', lines=True)
    
    # offer type as dummy columns
    portfolio_data = pd.get_dummies(portfolio, columns=['offer_type'])
    
    # portfolio `duration` in hours
    portfolio_data.duration = portfolio.duration.apply(lambda x: x*24)
    
    # rename `id` column as `offer_id`, and `duration` as `duration_hs`
    portfolio_data.rename(columns={'id':'offer_id', 'duration':'duration_hs'}, inplace=True)
    
    # reorder columns
    portfolio_data = portfolio_data[['offer_id',
                                    'channels',
                                    'duration_hs',
                                    'difficulty',
                                    'reward',
                                    'offer_type_bogo',
                                    'offer_type_discount',
                                    'offer_type_informational']]
    
    return portfolio_data


def get_profile():
    """ Retrieve and pre-process profile data.
    
    Pre-processing steps:
        * Profile genders are depicted in dummy columns.
        * Gender null values are depicted in `gender_na` column.
        * Income null values are replaced by the zero value.
        * `became_member_on` column is replaced by the `membership_days` column.
            The values are calculated as the number of days up to today.
        * `id` column is renamed as `profile_id`.
        * Columns are reordered.
    
    Return.
        Pandas Dataframe. Pre-processed profile data.
        
    """
    # read in the json file
    profile_data = pd.read_json('data/profile.json', orient='records', lines=True)
    
    # replace gender letters by more informative labels
    profile_data.gender = profile_data.gender.map({'M':'male', 'F':'female', 'O':'other'}, na_action='ignore')
    
    # fill in gender missing values
    profile_data.fillna(value={'gender':'na', 'income':0}, inplace=True)
    
    # gender as dummy columns
    profile_data = pd.get_dummies(profile_data, columns=['gender'], prefix='gender', prefix_sep='_')
    
    # generate `membership_days` columns from `became_member_on` column
    # membership_days values are calculated as the number of days up to today
    profile_data.became_member_on = pd.to_datetime(profile_data.became_member_on, format='%Y%m%d')
    profile_data['membership_days'] = profile_data.became_member_on.apply(lambda x: (datetime.today() - x).days)
    
    # drop `became_member_on` column
    profile_data.drop('became_member_on', axis=1, inplace=True)
    
    # rename `id` column as `profile_id`
    profile_data.rename(columns={'id':'profile_id'}, inplace=True)
    
    # reorder columns
    profile_data = profile_data[['profile_id',
                                   'age',
                                   'income',
                                   'membership_days',
                                   'gender_male',
                                   'gender_female',
                                   'gender_other',
                                   'gender_na']]
    
    return profile_data


def get_offers_transcript():
    """ Retrieve and pre-process offers transcript data.
    
    Pre-processing steps:
        * Trasaction events are discarded.
        * `person` column is renamed as `profile_id`.
        * `value` column is renamed as `offer_id`.
        * dictionary in renamed `offer_id` column is replaced by the offer id value.
        * `offer_expiration` columns is added depicting expiration time per offer received.
        * `offer_received` column is added depicting offer reception time.
        * `offer_viewed` column is added depicting offer viewing time (NaN if not viewed).
        * `offer_completed` column is added depicting offer completion time (NaN if not completed).
    
    Return.
        Pandas Dataframe. Pre-processed transcript data.
        
    """
    # read in the json file
    transcript = pd.read_json('data/transcript.json', orient='records', lines=True)
    
    # get transcript data only about offers
    offers_transcript = transcript[transcript.event != 'transaction'].copy()

    # rename the `person` column as `person_id`
    # rename the `value` column as `offer_id`
    offers_transcript.rename(columns={'person':'profile_id', 'value':'offer_id'}, inplace=True)

    # dict object in `offer_id` column is replaced by the value (offer id) as string
    offers_transcript.offer_id = offers_transcript.offer_id.apply(lambda x: list(x.values())[0])
    
    # offer event values cleaning
    offers_transcript.event = offers_transcript.event.map({'offer received':'offer_received',
                                                           'offer viewed':'offer_viewed',
                                                           'offer completed':'offer_completed'
                                                          })
    # remove duplicates
    offers_transcript.drop_duplicates(inplace=True)

    # get offers `expiration` time (in hours since offer received) 
    expiration_times = get_expiration_times(offers_transcript)

    # add expiration times as a column of transcripts data depicting offers
    offers_transcript['offer_expiration'] = expiration_times

    # pivot the table to get all the events time data per customer and per offer, in a single row
    offers_transcript = pd.pivot_table(offers_transcript,
                                       values='time',
                                       index=['profile_id', 'offer_id', 'offer_expiration'],
                                       columns='event')

    # reset the generated multi-index to default
    offers_transcript.reset_index(inplace=True)
    
    return offers_transcript


def get_expiration_times(offers_transcript):
    """ Set up the offer expiration time
    
    Args.
        offers_transcript (pandas dataframe) - Offers transcript data.
        
    Return.
        Pandas Series. Expiration time of the offers.
    
    """
    try: # if the expiration times are available, just retrieve them
        
        expiration_times = pd.read_csv('./data/expiration_times.csv', index_col=0).squeeze("columns")
        return expiration_times
        
    except FileNotFoundError: # otherwise, run the process to calculate them
        
        # get offers duration in hours
        portfolio = get_portfolio()
        offers_duration = pd.Series(portfolio.duration_hs.values, index=portfolio.offer_id)
        
        expiration_list = []
        # for each row in transcript data
        for _, offer in offers_transcript.iterrows():
        
            # if it's an `offer received` event, add up the duration to the event time
            if offer.event == 'offer_received':
                expiration = offers_duration[offer.offer_id] + offer.time
        
            # for `offer viewed` and `offer completed` events,
            # add up the duration to the corresponding offer reception event time
            else:
                # filter out the transcript data by person id, offer id, type of event,
                # and event occurring before/at the same time the current one
                # sort data by occurrence time in descending order
                offers_reception = offers_transcript.query(f"""profile_id == '{offer.profile_id}' and \
                                                            offer_id == '{offer.offer_id}' and \
                                                            event == 'offer_received' and \
                                                            time <= {offer.time}""").sort_values(by=['time'], ascending=False)

                # last offer reception event
                last_offer_reception_time = offers_reception.time.values[0]
                
                # add up the duration and the last event reception time
                expiration = offers_duration[offer.offer_id] + last_offer_reception_time
            
            expiration_list.append(expiration)
        
        # serialize the expiration times and return the data as a pandas series
        expiration_times = pd.Series(expiration_list, index=offers_transcript.index)
        expiration_times.to_csv('./data/expiration_times.csv')
        return expiration_times


def get_dataset():
    """ Merge pre-processed portfolio, profile and transcript data into a single dataset.
    
    Return.
        Pandas Dataframe. Single dataset.
    
    """
    # get pre processed portfolio, profile, and transcript data
    portfolio = get_portfolio()
    profile = get_profile()
    offers_transcript = get_offers_transcript()
    
    #perform merging
    dataset = offers_transcript.merge(profile, how='inner', on='profile_id') \
                                .merge(portfolio, how='inner', on='offer_id')
    
    return dataset
    
# def get_offer_received_time(transcript_data, person_id, offer_id, event_time):
#     """ Find the offer reception time of the argument offer event.
    
#     Args.
#         transcript_data (pandas dataframe) - Transcript data to process.
#         person_id (string) - Person's ID the offer was sent to.
#         offer_id (string) - Offer's ID of the current event.
#         event_time (int)  - Event time of the current event.
        
#     Return.
#         Integer. The reception time of the offer.
    
#     """
#     # filter out the transcript data by person id, offer id, type of event,
#     # and event occurring before/at the same time the current one
#     previous_times = transcript_data.query(f"""person == '{person_id}' 
#                                                 and offer == '{offer_id}' 
#                                                 and event == 'offer received'
#                                                 and time <= {event_time}""")
#     # sort data by occurrence time
#     previous_times.sort_values(by=['time'])
    
#     # return the last offer reception event before the current one
#     return previous_times.time[-1:].values[0]
    

# def get_offer_duration(portfolio_data, offer_id):
#     """ Return the duration of the argument offer.
    
#     Args.
#         portfolio_data (pandas dataframe) - Raw portfolio data.
#         offer_id (string) - Offer id.
        
#     Return.
#         Integer. Duration of the offer.
    
#     """
#     return portfolio_data.query(f"""id == '{offer_id}'""")['duration'].values[0]
    
    
# def get_single_table_data(profile_data, portfolio_data, transcript_data):
#     """ Create a single table data comprising demographic, portfolio, and offerings information.
    
#     Arg.
#         transcript (pandas dataframe) - Raw transcript data.
#         portfolio_data (pandas dataframe) - Raw portfolio data.
        
#     Return.
#         Pandas Dataframe.  A table data comprising demographic, portfolio, and offerings information.
    
    
#     """
#     # duration is depicted in hours
#     portfolio_hs = portfolio_data.copy()
#     portfolio_hs.duration = portfolio_hs.duration.apply(lambda x: x * 24)
    
#     # get a table depicting all the events time data per customer and per offer, in a single row
#     offers_table = get_offers_table(transcript_data, portfolio_hs)

#     # merge table of transcript offers and demographic data
#     person_offers_table = offers_table.merge(profile_data,
#                                              how='left',
#                                              left_on='person',
#                                              right_on='id',
#                                              suffixes=['_offer', '_person'])

#     # merge table of transcript offers with demographic data and portfolio data
#     person_porfolio_offers_table = person_offers_table.merge(portfolio_hs,
#                                                              how='left',
#                                                              left_on='offer',
#                                                              right_on='id',
#                                                              suffixes=['_person', '_portfolio'])


#     # filter out data about `informational` offers
#     person_porfolio_offers_table = person_porfolio_offers_table[person_porfolio_offers_table.offer_type != 'informational'].copy()

#     # drop unneeded columns
#     person_porfolio_offers_table.drop(['id_person',
#                                        'became_member_on',
#                                        'channels',
#                                        'id_portfolio'], axis=1, inplace=True)

#     # reorder columns
#     person_porfolio_offers_table = person_porfolio_offers_table[['person',
#                                                                  'gender',
#                                                                  'age',
#                                                                  'income',
#                                                                  'offer',
#                                                                  'offer_type',
#                                                                  'reward',
#                                                                  'difficulty',
#                                                                  'duration',
#                                                                  'expiration',
#                                                                  'offer received',
#                                                                  'offer viewed',
#                                                                  'offer completed']]
    
#     # rename columns
#     person_porfolio_offers_table.rename(columns={'person': 'person_id',
#                                                  'gender': 'person_gender',
#                                                  'age': 'person_age',
#                                                  'income': 'person_income',
#                                                  'offer': 'offer_id',
#                                                  'reward': 'offer_reward',
#                                                  'difficulty': 'offer_difficulty',
#                                                  'duration': 'offer_duration',
#                                                  'expiration': 'offer_expiration',
#                                                  'offer received': 'offer_received',
#                                                  'offer viewed': 'offer_viewed',
#                                                  'offer completed': 'offer_completed'}, inplace=True)
    
#     return person_porfolio_offers_table