from numpy import result_type
from sklearn.linear_model import LinearRegression
import pandas as pd

Lag = list(range(1, 13)) + list(range(14, 7 * 12 + 1, 7))
Rolling_mean_size = list(range(1, 11)) + list(range(14, 7 * 12 + 1, 7))


def sbs_predict(models, old_data, end_date, target_column, working_columns, mf_rules, only_negative=True):
    '''Выполнят прогноз построчно, позволяя использовать результаты предыдущего прогноза, для расчета признаков следующего. 

    Args:
        model: объект модели. !!!
        old_data: уже известные данные за прошлый период.
        end_date: дата, до которой рассчитать прогноз. Прогноз начнется со следующего дня после после old_data.
        target_column: имя колонки целевого признака.
        working_columns: имена колонок которые использовать для генерации фичей.
        only_negative: итоговый прогноз будет обнуляться, если модель выдаст значения больше нуля.
        lag: список размеров сдвига для создания признаков из значений предыдущего периода. !! todo переформулировать это
        rolling_mean_size: список размеров для создания признаков скользящего среднего

    Returns:
        Спрогнозированные значения.
    '''

    data = old_data[working_columns].copy()
    day_index = pd.date_range(data.index[-1], end_date)[1:]
    data = old_data.append(pd.DataFrame(
        [], columns=working_columns, index=day_index))

    for day in day_index:
        row = make_features(data.loc[:day], mf_rules
                            ).loc[[day]].drop(working_columns, axis=1)

        for column in working_columns:
            data.loc[day, column] = models[column].predict(row)[0]

    result = data.loc[day_index, target_column]
    if only_negative:
        result[result > 0] = 0
    return result


def default_mf_rules(working_columns, lag=Lag, rolling_mean_size=Rolling_mean_size):
    return [{'column': c, 'lag': lag, 'rolling_mean_size': rolling_mean_size} for c in working_columns]


def make_features(data, mf_rules):
    '''Рассчитывает дополнительные признаки для временного ряда. 

    Args:
        data: исходный временной ряд.
        column: имя колонки целевого признака.
        lag: список размеров сдвига для создания признаков из значений предыдущего периода. !! todo переформулировать это
        rolling_mean_size: список размеров для создания признаков скользящего среднего

    Returns:
        Копия датафрейма data в который добавлены новые признаки.
    '''

    data = data.copy()
    index = data.index
    # data['year'] = index.year
    data['month'] = index.month
    data['day'] = index.day
    data['dayofweek'] = index.dayofweek

    for rule in mf_rules:
        for l in rule['lag']:
            data[f'{rule["column"]}:lag:{l}'] = data[rule['column']].shift(l)

        for r in rule['rolling_mean_size']:
            data[f'{rule["column"]}:rm:{r}'] = data[rule['column']
                                                    ].shift().rolling(r).mean()

    return data


def create_models(data, working_columns, mf_rules):
    '''Генерирует признаки, создает и обучает новую модель. 

    Args:
        data: датафрейм временного ряда.
        target: имя колонки целевого признака.
        working_columns: имена колонок которые использовать для генерации фичей.
        lag: список размеров сдвига для создания признаков из значений предыдущего периода. !! todo переформулировать это
        rolling_mean_size: список размеров для создания признаков скользящего среднего

    Returns:
        Объект модели.
    '''
    train = make_features(data, mf_rules).dropna()

    models = {}
    for column in working_columns:
        models[column] = LinearRegression(
            n_jobs=-1).fit(train.drop(working_columns, axis=1), train[column])
    return models


def __create_models__(data, models, working_columns):
    train = make_features(data, working_columns, default_mf_rules(working_columns),
                          ).dropna()

    for column in working_columns:
        models[column] = models[column].fit(
            train.drop(working_columns, axis=1), train[column])
    return models
