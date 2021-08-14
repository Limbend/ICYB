from numpy import result_type
from sklearn.linear_model import LinearRegression
import pandas as pd

Lag = list(range(1, 13))+list(range(14, 7*12+1, 7))
Rolling_mean_size = list(range(1, 11))+list(range(14, 7*12+1, 7))


def sbs_predict(model, old_data, end_date, target_column, no_positiv=True, lag=Lag, rolling_mean_size=Rolling_mean_size):
    data = old_data[[target_column]].copy()
    day_index = pd.date_range(data.index[-1], end_date)[1:]
    data = old_data.append(pd.DataFrame(
        [], columns=[target_column], index=day_index))

    for day in day_index:
        row = make_features(data.loc[:day], target_column,
                            lag,
                            rolling_mean_size
                            ).loc[[day]].drop(target_column, axis=1)

        data.loc[day, target_column] = model.predict(row)[0]

    result = data.loc[day_index, target_column]
    if no_positiv:
        result[result > 0] = 0
    return result


def make_features(data, target, lag, rolling_mean_size):
    data = data.copy()
    index = data.index
    # data['year'] = index.year
    data['month'] = index.month
    data['day'] = index.day
    data['dayofweek'] = index.dayofweek

    if type(lag) is list:
        for l in lag:
            data['lag_{}'.format(l)] = data[target].shift(l)
    else:
        for l in range(1, lag + 1):
            data['lag_{}'.format(l)] = data[target].shift(l)

    if type(rolling_mean_size) is list:
        for r in rolling_mean_size:
            data[f'rolling_mean_{r}'] = data[target].shift().rolling(r).mean()
    else:
        data[f'rolling_mean_{rolling_mean_size}'] = data[target].shift().rolling(
            rolling_mean_size).mean()

    return data


def create_model(data, target, lag=Lag, rolling_mean_size=Rolling_mean_size):
    train = make_features(data, target,
                          lag,
                          rolling_mean_size
                          ).dropna()

    return LinearRegression(n_jobs=7).fit(train.drop(target, axis=1), train[target])
