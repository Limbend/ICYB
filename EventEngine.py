import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta

def get_balance_past(start, costs):
    result = costs.cumsum()
    return result + (start - result.iloc[-1])

def get_balance_future(start, costs):
    return costs.cumsum()+start

def get_markers_regular(data, event):
    if event['search_f'] == 'description':
        return data['description'] == event['arg_sf']
        
    elif event['search_f'] == 'amount_description':
        return (data['description'] == event['arg_sf']) & (data['amount'] == event['amount'])

    elif event['search_f'] == 'amount<_description':
        arg_sf = event['arg_sf'].split(',')
        return (data['description'] == arg_sf[1]) & (data['amount'] < int(arg_sf[0]))

    elif event['search_f'] == 'amount_category':
        return (data['category'] == event['arg_sf']) & (data['amount'] == event['amount'])

    elif event['search_f'] == 'amount<_category':
        arg_sf = event['arg_sf'].split(',')
        return (data['category'] == arg_sf[1]) & (data['amount'] < int(arg_sf[0]))

    elif event['search_f'] == 'dont_search':
        return np.full(len(data), False)
    
        
    raise Exception(f'The search function /"{event["search_f"]}/" does not exist')

def get_updated_regular(data, regular_events, window_price=3, uniform_distribution=False):
    new_regular_events = regular_events.copy()
    
    for i in regular_events[regular_events['adjust_price']].index:
        amounts = data[get_markers_regular(data, regular_events.loc[i])].tail(window_price)['amount']
        if uniform_distribution:
            new_regular_events.loc[i, 'amount'] = amounts.mean()
        else:
            new_regular_events.loc[i, 'amount'] = (amounts * [2 / (window_price + window_price**2) * (x + 1) for x in range(window_price)]).sum()
            
    for i in regular_events[regular_events['adjust_date']].index:
        events = data[get_markers_regular(data, regular_events.loc[i])]
        
        if len(events)>0: 
            new_regular_events.loc[i, 'start_date'] = events.iloc[-1]['date']
        
    return new_regular_events

def get_regular_events(regular_events, g_start_date, g_end_date):

    result = []
    j_limit=1000    
    regular_events = regular_events.copy()
    
    for i in regular_events.index:
        r_event = regular_events.loc[i]
        j = 0
        if r_event['end_date'] is None:
            end_date = g_end_date
        else:
            end_date = min(r_event['end_date'], g_end_date)
            
        d_date = relativedelta(
            years=int(r_event['d_years']),
            months=int(r_event['d_months']),
            days=int(r_event['d_days'])
        )
        
        date = r_event['start_date']
        
        # Поиск начальной даты
        while(date < g_start_date and date < end_date and j<j_limit):            
            date += d_date
            j+=1
            
        if j==j_limit:
            raise Exception(f'When searching for the start date, the maximum number of iterations was exceeded\n{regular_events.loc[i]}')
        
        
        j = 0      
        while(date < end_date and j<j_limit):
            result.append((
                date,
                r_event['amount'],
                f'regular[{i}]',
                r_event['description'],
                np.nan
            ))
            date += d_date
            j+=1
            
        if j==j_limit:
            raise Exception(f'The maximum number of iterations has been exceeded\n{regular_events.loc[i]}')
            
    return pd.DataFrame(result, columns = ['date', 'amount', 'category', 'description', 'balance']).sort_values('date').reset_index(drop=True)

def get_full_costs_list(new_costs, db_engine, user_id):
    old_costs = db_engine.download_costs(user_id)
    old_costs['is_new'] = False
    new_costs = new_costs.copy()
    new_costs['is_new'] = True

    return pd.concat([old_costs, new_costs]).drop_duplicates(subset=['date', 'amount']).sort_values('date')

def save_new_costs(full_costs, db_engine, user_id):
    if (len(full_costs[~full_costs['is_new']])>0) and \
    (len(full_costs[full_costs['is_new']])>0) and \
    (full_costs[~full_costs['is_new']].iloc[-1]['date'] > full_costs[full_costs['is_new']].iloc[0]['date']):

        #todo позже добавить обработку этого события !!!
        raise Exception(f'Events added retroactively were detected. Such a database update has not yet been implemented. The change has not been entered into the database!')

    db_engine.add_costs(full_costs[full_costs['is_new']], user_id=user_id)
    return (full_costs['is_new'].sum(), 0)

