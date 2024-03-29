from os import X_OK
import pandas as pd
import numpy as np

from tqdm import tqdm

# from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.dummy import DummyRegressor

from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from urllib3 import Retry
from sklearn.inspection import permutation_importance

import ML as ml
import EventEngine as ee

GRS = 7182818


def sum_score(y_true, y_pred):
    return abs(y_true.sum() - y_pred.sum())


def model_test(sbs_model, train, test, label='main_model'):
    sbs_model.fit(train)
    predict = sbs_model.predict_full(train, test.index[-1])

    result = {
        'label': label,
        'y_predict': predict[sbs_model.target_column],
        'target_rmse': mean_squared_error(test[sbs_model.target_column], predict[sbs_model.target_column], squared=False),
        'target_sum_score': sum_score(test[sbs_model.target_column], predict[sbs_model.target_column]),
    }
    for column in set(sbs_model.list_mf_rules.keys()) - set([sbs_model.target_column]):
        result[column + '_rmse'] = mean_squared_error(
            test[column], predict[column], squared=False)

    return result


def dummy_model_test(train, test, target, label='dummy_model'):
    dummy_model = DummyRegressor(strategy='median').fit(
        train.drop(target, axis=1), train[target])
    dummy_predict = dummy_model.predict(test)

    return {
        'label': label,
        'rmse': mean_squared_error(test[target], dummy_predict, squared=False),
        'sum_score': sum_score(test[target], dummy_predict),
        # 'y_predict': dummy_predict,
    }


def iterative_model_test(sbs_model, transactions, regular_list):
    data = ee.preprocessing_for_ml(transactions, regular_list, sbs_model)
    full_data = ee.preprocessing_for_ml(transactions, regular_list[0:0], sbs_model)

    full_months = int(
        (data.index[-1] - data.index[0]) / np.timedelta64(1, 'M'))

    months_minimum = 3
    if(full_months < months_minimum):
        raise Exception(
            f'Not enough data for test. The number of full months {full_months}. It is necessary to minimum {months_minimum}.')

    result = []
    for months_train in range(months_minimum, full_months):
        test_date = data.index[0] + relativedelta(months=months_train)

        step_info = {
            'months_train': months_train,
            'train_size': len(data[:test_date - relativedelta(days=1)]),
        }

        # ===============================
        # test main_model 'isfull': False
        # ===============================
        test_result = model_test(
            sbs_model,
            data[:test_date - relativedelta(days=1)],
            data[test_date:test_date + relativedelta(months=1)],
            'main_model'
        )
        test_result.update(step_info)
        test_result.update({'isfull': False})
        y_predict = test_result['y_predict'].to_frame()
        del test_result['y_predict']
        result.append(test_result)

        # ==============================
        # test main_model 'isfull': True
        # ==============================
        regular_events = ee.get_regular_events(
            regular_list, transactions, test_date, test_date + relativedelta(months=1)).set_index('date')
        # Работаем толбко с затратами
        regular_events = regular_events[regular_events['amount'] < 0]

        y_predict = pd.concat([
            regular_events[['amount']],
            y_predict
        ]).resample('1D').sum()

        test_result = {
            'label': 'main_model',
            'rmse': mean_squared_error(data[test_date:test_date + relativedelta(months=1)]['amount'], y_predict['amount'], squared=False),
            'sum_score': sum_score(data[test_date:test_date + relativedelta(months=1)]['amount'], y_predict['amount']),
            'isfull': True
        }
        test_result.update(step_info)
        result.append(test_result)

        # ================================
        # test dummy_model 'isfull': False
        # ================================
        test_result = dummy_model_test(
            data[:test_date - relativedelta(days=1)],
            data[test_date:test_date + relativedelta(months=1)],
            'amount'
        )
        test_result.update(step_info)
        test_result.update({'isfull': False})
        result.append(test_result)

        # ===============================
        # test dummy_model 'isfull': True
        # ===============================
        test_result = dummy_model_test(
            full_data[:test_date - relativedelta(days=1)],
            full_data[test_date:test_date + relativedelta(months=1)],
            'amount'
        )
        test_result.update(step_info)
        test_result.update({'isfull': True})
        result.append(test_result)

    return pd.DataFrame(result)


def get_importances(sbs_model, X, random_state=GRS):
    results = []

    for column in sbs_model.models.keys():
        x = sbs_model.make_features(
            X, sbs_model.list_mf_rules[column]).dropna()
        y_working_columns = x[sbs_model.models.keys()]
        x = x.drop(sbs_model.models.keys(), axis=1)

        # Говнокод! TODO переделать
        feature_split = []
        for feature in list(x):
            split = feature.split(':')
            if len(split) == 3:
                feature_split.append(split)
            else:
                feature_split.append([np.nan, np.nan, np.nan])

        y = y_working_columns[column]

        importance = permutation_importance(
            sbs_model.models[column], x, y, n_repeats=10, random_state=random_state, n_jobs=-1, scoring='neg_root_mean_squared_error')

        result = pd.DataFrame(feature_split, columns=[
                              'root_f', 'type', 'value'])
        result['feature'] = list(x)
        result['importances_mean'] = importance.importances_mean
        result['importance_for'] = column

        results.append(result)

    return pd.concat(results)


def estimate_mf_rules(train, sbs_model, values, step_size=5, best_list_size=2):
    result = pd.DataFrame([], columns=['root_f', 'type',
                          'value', 'feature', 'importances_mean', 'importance_for', 'steps'])
    steps = len(values) // step_size + \
        (0 if len(values) % step_size == 0 else 1)

    for i in tqdm(range(steps)):
        if i == steps-1:
            v = values[i*step_size: -1]
        else:
            v = values[i*step_size: (i+1)*step_size]

        list_mf_rules = {c2: [{
            'column': c,
            'lag': v + list(result[(result['root_f'] == c) & (result['type'] == 'lag') & (result['importance_for'] == c2)].sort_values(by='importances_mean', ascending=False).head(best_list_size)['value'].apply(int).values),
            'rm': v + list(result[(result['root_f'] == c) & (result['type'] == 'rm') & (result['importance_for'] == c2)].sort_values(by='importances_mean', ascending=False).head(best_list_size)['value'].apply(int).values)
        } for c in sbs_model.list_mf_rules.keys()] for c2 in sbs_model.list_mf_rules.keys()}

        sbs_model.list_mf_rules = list_mf_rules
        sbs_model.fit(train)

        importances = get_importances(sbs_model, train)
        importances['steps'] = i
        result = pd.concat([result, importances])

    return result.sort_values(by='importances_mean', ascending=False).drop_duplicates(subset=['type', 'value', 'importance_for'])


def top_list_mf_rules(importances, working_columns, size=50):
    importances = importances.groupby('importance_for').head(size)

    list_mf_rules = {c2: [{
        'column': c,
        'lag': list(importances[(importances['root_f'] == c) & (importances['type'] == 'lag') & (importances['importance_for'] == c2)]['value'].apply(int).values),
        'rm': list(importances[(importances['root_f'] == c) & (importances['type'] == 'rm') & (importances['importance_for'] == c2)]['value'].apply(int).values)
    } for c in working_columns] for c2 in working_columns}

    return list_mf_rules


def list_mf_rules_test(train, test, sbs_model, r1=range(1, 101), r2=range(1, 70), step_size=5, best_list_size=1):
    importances = estimate_mf_rules(
        train, sbs_model, list(r1), step_size, best_list_size)

    test_r2 = []
    for size in tqdm(r2):
        sbs_model.list_mf_rules = top_list_mf_rules(
            importances, sbs_model.list_mf_rules.keys(), size)
        test_result = model_test(sbs_model, train, test)

        test_result.pop('label', None)
        test_result.pop('y_predict', None)
        test_result.update({
            'mf_rules_size': size,
            'list_mf_rules': sbs_model.list_mf_rules
        })

        test_r2.append(test_result)

    return pd.DataFrame(test_r2)
