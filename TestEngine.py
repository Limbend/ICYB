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


def sum_score(y_true, y_pred):
    return abs(y_true.sum() - y_pred.sum())


def model_test(train, test, target, working_columns, mf_rules, label='main_model'):
    models = ml.create_models(train, working_columns, mf_rules)
    predict = ml.sbs_predict(
        models, train, test.index[-1], target, working_columns, mf_rules)

    return {
        'label': label,
        'rmse': mean_squared_error(test[target], predict, squared=False),
        'sum_score': sum_score(test[target], predict),
        'y_predict': predict,
    }


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


def iterative_model_test(costs, regular_list, start_date, add_column=False):
    working_columns = ['amount'] if not(add_column) else [
        'amount', 'category_n', 'description_n']

    data = ee.preprocessing_for_ml(
        costs, regular_list, start_date, add_column=add_column)
    full_data = ee.preprocessing_for_ml(
        costs, regular_list[0:0], start_date, add_column=False)

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
            data[:test_date - relativedelta(days=1)],
            data[test_date:test_date + relativedelta(months=1)],
            'amount',
            working_columns,
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
            regular_list, costs, test_date, test_date + relativedelta(months=1)).set_index('date')
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


def get_importances(models, X, target, working_columns, mf_rules):
    x = ml.make_features(X, mf_rules).dropna()
    y = x[target]
    x = x.drop(working_columns, axis=1)

    importance = permutation_importance(
        models[target], x, y, n_repeats=10, random_state=42, n_jobs=-1)

    # Говнокод! todo переделать
    feature_split = []
    for feature in list(x):
        split = feature.split(':')
        if len(split) == 3:
            feature_split.append(split)
        else:
            feature_split.append([np.nan, np.nan, np.nan])

    result = pd.DataFrame(feature_split, columns=['root_f', 'type', 'value'])
    result['feature'] = list(x)
    result['importances_mean'] = importance.importances_mean
    return result


def estimate_mf_rules(train, target, working_columns, values, step_size=10, best_list_size=5):
    result = pd.DataFrame([], columns=['root_f', 'type',
                          'value', 'feature', 'importances_mean', 'steps'])
    steps = len(values) // step_size + \
        (0 if len(values) % step_size == 0 else 1)

    for i in range(steps):
        if i == steps-1:
            v = values[i*step_size: -1]
        else:
            v = values[i*step_size: (i+1)*step_size]

        mf_rules = [{
            'column': c,
            'lag': v + list(result[(result['root_f'] == c) & (result['type'] == 'lag')].sort_values(by='importances_mean', ascending=False).head(best_list_size)['value'].apply(int).values),
            'rolling_mean_size': v + list(result[(result['root_f'] == c) & (result['type'] == 'rm')].sort_values(by='importances_mean', ascending=False).head(best_list_size)['value'].apply(int).values)
        } for c in working_columns]

        models = ml.create_models(train, working_columns, mf_rules)

        importances = get_importances(
            models, train, target, working_columns, mf_rules)
        importances['steps'] = i
        result = pd.concat([result, importances])

    return result


def top_mf_rules(importances, working_columns, size=50):
    importances = importances.copy().sort_values(
        by='importances_mean', ascending=False).drop_duplicates(subset=['type', 'value']).head(size)

    mf_rules = [{
        'column': c,
        'lag': list(importances[(importances['root_f'] == c) & (importances['type'] == 'lag')]['value'].apply(int).values),
        'rolling_mean_size': list(importances[(importances['root_f'] == c) & (importances['type'] == 'rm')]['value'].apply(int).values)
    } for c in working_columns]

    return mf_rules


def set_mf_rules_test(train, test, target, working_columns, r1=range(1, 101), r2=range(1, 70)):
    importances = estimate_mf_rules(train, target, working_columns, list(r1))

    test_r2 = []
    for size in tqdm(r2):
        mf_rules = top_mf_rules(importances, working_columns, size)
        test_result = model_test(
            train, test, target, working_columns, mf_rules)

        test_r2.append({
            'mf_rules_size': size,
            'rmse': test_result['rmse'],
            'sum_score': test_result['sum_score'],
            'mf_rules': mf_rules
        })

    return pd.DataFrame(test_r2)
