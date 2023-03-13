from sklearn.linear_model import LinearRegression
import pandas as pd


class SbsModel:
    '''Класс модели, выполняющий прогноз построчно, позволяя использовать результаты предыдущего прогноза, для расчета признаков следующего.

    Attributes:
        models: словарь моделией под каждую фичу.
        target_column: имя колонки целевого признака.
        column_adding_method: медод генерации новых фич.
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
    '''

    def __init__(self, target_column, column_adding_method, list_mf_rules):
        self.target_column = target_column
        self.column_adding_method = column_adding_method
        self.list_mf_rules = list_mf_rules

    def predict(self, old_data, end_date, only_negative=True):
        '''Выполнят прогноз построчно, позволяя использовать результаты предыдущего прогноза, для расчета признаков следующего. 

        Args:
            old_data: уже известные данные за прошлый период.
            end_date: дата, до которой рассчитать прогноз. Прогноз начнется со следующего дня после после old_data.
            only_negative: итоговый прогноз будет обнуляться, если модель выдаст значения больше нуля.

        Returns:
            Спрогнозированные значения.
        '''

        return self.predict_full(old_data, end_date, only_negative)[self.target_column]

    def predict_full(self, old_data, end_date, only_negative=True):
        '''Выполнят прогноз построчно, позволяя использовать результаты предыдущего прогноза, для расчета признаков следующего.

        Args:
            old_data: уже известные данные за прошлый период.
            end_date: дата, до которой рассчитать прогноз. Прогноз начнется c следующего дня после после old_data.
            only_negative: итоговый прогноз будет обнуляться, если модель выдаст значения больше нуля.

        Returns:
            Спрогнозированные значения, для всех фич.
        '''
        working_columns = self.models.keys()
        data = old_data[working_columns].copy()
        days_index = pd.date_range(data.index[-1], end_date)[1:]
        data = pd.concat(
            [data, pd.DataFrame([], columns=working_columns, index=days_index)])

        for day in days_index:
            for column in working_columns:
                row = self.make_features(data.loc[:day], self.list_mf_rules[column]
                                         ).loc[[day]].drop(working_columns, axis=1)

                data.loc[day, column] = self.models[column].predict(row)[0]

        result = data.loc[days_index, working_columns]
        if only_negative:
            result[self.target_column][result[self.target_column] > 0] = 0
        return result

    def make_features(self, data, mf_rules):
        '''Рассчитывает дополнительные признаки для временного ряда. 

        Args:
            data: исходный временной ряд.
            column: имя колонки целевого признака.
            mf_rules: список правил для генерации фичей, формата:
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
                data[f'{rule["column"]}:lag:{l}'] = data[rule['column']].shift(
                    l)

            for r in rule['rm']:
                data[f'{rule["column"]}:rm:{r}'] = data[rule['column']
                                                        ].shift().rolling(r).mean()

        return data

    def fit(self, data):
        '''Генерирует признаки, создает и обучает новую модель под каждую фичу.

        Args:
            data: датафрейм временного ряда.
        '''
        models = {}
        for column in self.list_mf_rules.keys():
            train = self.make_features(
                data, self.list_mf_rules[column]).dropna()
            models[column] = LinearRegression(
                n_jobs=-1).fit(train.drop(self.list_mf_rules.keys(), axis=1), train[column])

        self.models = models

        return self
