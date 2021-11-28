import pandas as pd
import numpy as np
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import ML as ml


def get_balance_past(start, costs):
    result = costs.cumsum()
    return result + (start - result.iloc[-1])


def get_balance_future(start, costs):
    return costs.cumsum() + start


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

    raise Exception(
        f'The search function /"{event["search_f"]}/" does not exist')


def get_regular_events(regular_events, costs, g_start_date, g_end_date, window_price=3, uniform_distribution=False):
    new_regular_events = regular_events.copy()
    result = []
    j_limit = 1000

    for i, r_event in new_regular_events.iterrows():
        d_date = relativedelta(
            years=int(r_event['d_years']),
            months=int(r_event['d_months']),
            days=int(r_event['d_days'])
        )

        if r_event['end_date'] is None:
            r_event['end_date'] = g_end_date
        else:
            r_event['end_date'] = min(r_event['end_date'], g_end_date)

        # Обновление цены
        if(r_event['adjust_price']):
            amounts = costs[get_markers_regular(costs, r_event)].tail(window_price)[
                'amount']
            if(len(amounts) > 0):
                if uniform_distribution:
                    r_event['amount'] = amounts.mean()
                else:
                    window_price = min(window_price, len(amounts))
                    r_event['amount'] = (
                        amounts * [2 / (window_price + window_price**2) * (x + 1) for x in range(window_price)]).sum()

        # Обновление начальной даты
        if(r_event['adjust_date']):
            events = costs[get_markers_regular(costs, regular_events.loc[i])]
            if len(events) > 0:
                r_event['start_date'] = events.iloc[-1]['date']

        j = 0
        new_start = r_event['start_date']  # + d_date * j
        while(new_start < g_start_date and new_start < r_event['end_date']):
            j += 1
            if j == j_limit:
                raise Exception(
                    f'When searching for the start date, the maximum number of iterations was exceeded\n{r_event}')

            new_start = r_event['start_date'] + d_date * j

        # Проверка на просрочку
        j -= 1
        if(r_event['follow_overdue'] and j > 0):
            pay_date_overdue = g_start_date + relativedelta(days=1)

            if(r_event['adjust_date']):
                # Если между стартовой датой для регулярки r_event['start_date'] и стартовой датой для начала поиска g_start_date есть помешаются регулярки - они являются просрочкой. Количество поместившихся будет в j.
                for i_overdue in range(j):
                    result.append((
                        pay_date_overdue,
                        r_event['amount'],
                        f'overdue[{i}-{i_overdue}]',
                        r_event['description'],
                        np.nan,
                        True
                    ))
            else:
                # Посчитать сколько должно быть регулярок между стартовой датой r_event['start_date'] и начальной датой поиска g_start_date.
                # Вычесть из них сколько по факту было.

                count_overdue = j - sum(get_markers_regular(
                    costs[costs['date'] >= pd.to_datetime(r_event['start_date'])], r_event)) + 1
                # Если число положительное, то есть просрочки.
                if(count_overdue > 0):
                    for i_overdue in range(count_overdue):
                        result.append((
                            pay_date_overdue,
                            r_event['amount'],
                            f'overdue[{i}-{i_overdue}]',
                            r_event['description'],
                            np.nan,
                            True
                        ))

                # Если отрицательное, то есть оплата зарание. Нужно обновить стартовую дату.
                elif(count_overdue < 0):
                    new_start = r_event['start_date'] + \
                        d_date * (j - count_overdue)

        j = 0
        date = new_start  # + d_date * j
        while(date < r_event['end_date']):
            result.append((
                date,
                r_event['amount'],
                f'regular[{i}]',
                r_event['description'],
                np.nan,
                False
            ))

            j += 1
            if j == j_limit:
                raise Exception(
                    f'The maximum number of iterations has been exceeded\n{r_event}')
            date = new_start + d_date * j

    df_events = pd.DataFrame(result, columns=[
                             'date', 'amount', 'category', 'description', 'balance', 'is_overdue']).sort_values('date').reset_index(drop=True)
    df_events['date'] = pd.to_datetime(df_events['date'])
    return df_events


def get_all_costs(new_costs, db_engine, user_id):
    old_costs = db_engine.download_costs(user_id)
    old_costs['is_new'] = False
    new_costs = new_costs.copy()
    new_costs['is_new'] = True

    return pd.concat([old_costs, new_costs]).drop_duplicates(subset=['date', 'amount']).sort_values('date')


def save_new_costs(full_costs, db_engine, user_id):
    if (len(full_costs[~full_costs['is_new']]) > 0) and \
        (len(full_costs[full_costs['is_new']]) > 0) and \
            (full_costs[~full_costs['is_new']].iloc[-1]['date'] > full_costs[full_costs['is_new']].iloc[0]['date']):

        # todo позже добавить обработку этого события !!!
        raise Exception(
            f'Events added retroactively were detected. Such a database update has not yet been implemented. The change has not been entered into the database!')

    db_engine.add_costs(full_costs[full_costs['is_new']], user_id=user_id)
    return (full_costs['is_new'].sum(), 0)


def drop_paired(data: pd.DataFrame, by: str):
    sort_values = data[by].sort_values()
    abs_values = sort_values.abs()
    c1 = sort_values.groupby(abs_values).transform(pd.Series.cumsum) > 0
    c2 = sort_values[::-1].groupby(abs_values).transform(pd.Series.cumsum) < 0

    return data[c1 | c2]


def drop_outliers(data, q=0.16):
    data = data[data['amount'] > data['amount'].quantile(q)]
    return data


def preprocessing_for_ml(data, regular_list, start_date, q=0.16):
    cleared_df = data.copy()
    cleared_df = cleared_df[cleared_df['date'] > start_date]
    cleared_df = drop_paired(cleared_df, 'amount')

    # Выделяет только расходы
    markers = cleared_df['amount'] < 0
    for i in regular_list.index:
        markers = markers & ~get_markers_regular(
            cleared_df, regular_list.loc[i])
    cleared_df = cleared_df[markers]

    cleared_df = drop_outliers(cleared_df, q)

    cleared_df = cleared_df.set_index('date')[['amount']].resample('1D').sum()

    return cleared_df


def predict_regular_events(regular_list, costs, end_date):
    balance = costs['balance'].iloc[-1]
    predicted_regular = get_regular_events(
        regular_list, costs, date.today(), end_date)

    predicted_regular['balance'] = get_balance_future(
        balance, predicted_regular['amount'])
    return predicted_regular.set_index('date')


def get_full_costs(predicted_regular, clear_costs, balance, model, end_date):
    full_costs = pd.concat([
        predicted_regular[['amount']],

        ml.sbs_predict(
            model,
            clear_costs,
            end_date,
            'amount'

        ).to_frame()
    ]).resample('1D').sum()

    full_costs['balance'] = get_balance_future(balance, full_costs['amount'])

    return full_costs


def fit_new_model(data):
    return ml.create_model(data, 'amount')
