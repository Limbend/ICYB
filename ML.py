# from numpy import result_type !!!
from sklearn.linear_model import LinearRegression
import pandas as pd


def sbs_predict(models, old_data, end_date, target_column, working_columns, list_mf_rules, only_negative=True):
    '''Выполнят прогноз построчно, позволяя использовать результаты предыдущего прогноза, для расчета признаков следующего. 

    Args:
        models: словарь моделией под каждую фичу.
        old_data: уже известные данные за прошлый период.
        end_date: дата, до которой рассчитать прогноз. Прогноз начнется со следующего дня после после old_data.
        target_column: имя колонки целевого признака.
        working_columns: имена колонок которые использовать для генерации фичей.
        list_mf_rules: список правил для генерации фичей.
        only_negative: итоговый прогноз будет обнуляться, если модель выдаст значения больше нуля.

    Returns:
        Спрогнозированные значения.
    '''

    return sbs_predict_full(models, old_data, end_date, target_column, working_columns, list_mf_rules, only_negative)[target_column]


def sbs_predict_full(models, old_data, end_date, target_column, working_columns, list_mf_rules, only_negative=True):
    '''Выполнят прогноз построчно, позволяя использовать результаты предыдущего прогноза, для расчета признаков следующего. 

    Args:
        models: словарь моделией под каждую фичу.
        old_data: уже известные данные за прошлый период.
        end_date: дата, до которой рассчитать прогноз. Прогноз начнется со следующего дня после после old_data.
        target_column: имя колонки целевого признака.
        working_columns: имена колонок которые использовать для генерации фичей.
        list_mf_rules: список правил для генерации фичей.
        only_negative: итоговый прогноз будет обнуляться, если модель выдаст значения больше нуля.

    Returns:
        Спрогнозированные значения, для всех фич.
    '''

    data = old_data[working_columns].copy()
    days_index = pd.date_range(data.index[-1], end_date)[1:]
    data = old_data.append(pd.DataFrame(
        [], columns=working_columns, index=days_index))

    for day in days_index:
        for column in working_columns:
            row = make_features(data.loc[:day], list_mf_rules[column]
                                ).loc[[day]].drop(working_columns, axis=1)

            data.loc[day, column] = models[column].predict(row)[0]

    result = data.loc[days_index, working_columns]
    if only_negative:
        result[target_column][result[target_column] > 0] = 0
    return result


def make_features(data, mf_rules):
    '''Рассчитывает дополнительные признаки для временного ряда. 

    Args:
        data: исходный временной ряд.
        column: имя колонки целевого признака.
        list_mf_rules: список правил для генерации фичей, формата:
            [
                {'column': 'column_name_1', 'lag': [...], 'rm': [...]},
                {'column': 'column_name_2', 'lag': [...], 'rm': [...]},
                ...
            ]

            lag: список сдвигов.
            rm: список размеров скользящего среднего.

    Returns:
        Копия датафрейма data в который добавлены новые признаки.
    '''

    data = data.copy()
    index = data.index
    data['year'] = index.year
    data['month'] = index.month
    data['day'] = index.day
    data['dayofweek'] = index.dayofweek

    for rule in mf_rules:
        for l in rule['lag']:
            data[f'{rule["column"]}:lag:{l}'] = data[rule['column']].shift(l)

        for r in rule['rm']:
            data[f'{rule["column"]}:rm:{r}'] = data[rule['column']
                                                    ].shift().rolling(r).mean()

    return data


def create_models(data, working_columns, list_mf_rules):
    '''Генерирует признаки, создает и обучает новую модель под каждую фичу. 

    Args:
        data: датафрейм временного ряда.
        working_columns: имена колонок которые использовать для генерации фичей.
        list_mf_rules: список правил для генерации фичей, под каждую модель, формата:
            {
                'column_name_1': [
                    {'column': 'column_name_1', 'lag': [...], 'rm': [...]},
                    {'column': 'column_name_2', 'lag': [...], 'rm': [...]},
                    ...
                ],
                'column_name_2': [
                    {'column': 'column_name_1', 'lag': [...], 'rm': [...]},
                    {'column': 'column_name_2', 'lag': [...], 'rm': [...]},
                    ...
                ],
                ...
            }

            lag: список сдвигов.
            rm: список размеров скользящего среднего.


    Returns:
        словарь моделией под каждую фичу.
    '''
    models = {}
    for column in working_columns:
        train = make_features(data, list_mf_rules[column]).dropna()
        models[column] = LinearRegression(
            n_jobs=-1).fit(train.drop(working_columns, axis=1), train[column])
    return models
