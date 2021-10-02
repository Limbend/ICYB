from numpy import result_type
from sklearn.linear_model import LinearRegression
import pandas as pd

Lag = list(range(1, 13))+list(range(14, 7*12+1, 7))
Rolling_mean_size = list(range(1, 11))+list(range(14, 7*12+1, 7))


def sbs_predict(model, old_data, end_date, target_column, only_negative=True, lag=Lag, rolling_mean_size=Rolling_mean_size):
    '''Выполнят прогноз построчно, позволяя использовать результаты предыдущего прогноза, для расчета признаков следующего. 

    Args:
        model: объект модели.
        old_data: уже известные данные за прошлый период.
        end_date: дата, до которой рассчитать прогноз. Прогноз начнется со следующего дня после после old_data.
        target_column: имя колонки целевого признака.
        only_negative: итоговый прогноз будет обнуляться, если модель выдаст значения больше нуля.
        lag: список размеров сдвига для создания признаков из значений предыдущего периода. !! todo переформулировать это
        rolling_mean_size: список размеров для создания признаков скользящего среднего

    Returns:
        Спрогнозированные значения.
    '''

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
    if only_negative:
        result[result > 0] = 0
    return result


def make_features(data, target, lag, rolling_mean_size):
    '''Рассчитывает дополнительные признаки для временного ряда. 

    Args:
        data: исходный временной ряд.
        target: имя колонки целевого признака.
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

    if type(lag) is list:
        for l in lag:
            data[f'lag_{l}'] = data[target].shift(l)
    else:
        for l in range(1, lag + 1):
            data[f'lag_{l}'] = data[target].shift(l)

    if type(rolling_mean_size) is list:
        for r in rolling_mean_size:
            data[f'rolling_mean_{r}'] = data[target].shift().rolling(r).mean()
    else:
        data[f'rolling_mean_{rolling_mean_size}'] = data[target].shift().rolling(
            rolling_mean_size).mean()

    return data


def create_model(data, target, lag=Lag, rolling_mean_size=Rolling_mean_size):
    '''Генерирует признаки, создает и обучает новую модель. 

    Args:
        data: датафрейм временного ряда.
        target: имя колонки целевого признака.
        lag: список размеров сдвига для создания признаков из значений предыдущего периода. !! todo переформулировать это
        rolling_mean_size: список размеров для создания признаков скользящего среднего

    Returns:
        Объект модели.
    '''
    train = make_features(data, target,
                          lag,
                          rolling_mean_size
                          ).dropna()

    return LinearRegression(n_jobs=7).fit(train.drop(target, axis=1), train[target])
