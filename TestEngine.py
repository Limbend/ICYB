import pandas as pd
import numpy as np

# from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.dummy import DummyRegressor

from datetime import date, datetime
from dateutil.relativedelta import relativedelta

import ML as ml
import EventEngine as ee


def sum_score(y_true, y_pred):
    return abs(y_true.sum() - y_pred.sum())


def model_test(train, test, target, label='main_model', lag=ml.Lag, rolling_mean_size=ml.Rolling_mean_size):
    model = ml.create_model(train, target, lag, rolling_mean_size)
    predict = ml.sbs_predict(model, train, test.index[-1], target)

    return {
        'label': label,
        'rmse': mean_squared_error(test[target], predict, squared=False),
        'sum_score': sum_score(test[target], predict),
        'y_predict': predict,
    }


# , lag=Lag, rolling_mean_size=Rolling_mean_size):
def dummy_model_test(train, test, target, label='dummy_model'):
    # train = make_features(train, target,
    #                       lag,
    #                       rolling_mean_size
    #                       ).dropna()
    dummy_model = DummyRegressor(strategy='median').fit(
        train.drop(target, axis=1), train[target])
    dummy_predict = dummy_model.predict(test)

    return {
        'label': label,
        'rmse': mean_squared_error(test[target], dummy_predict, squared=False),
        'sum_score': sum_score(test[target], dummy_predict),
        # 'y_predict': dummy_predict,
    }


def iterative_model_test(costs, regular_list, start_date):
    data = ee.preprocessing_for_ml(costs, regular_list, start_date)
    full_data = ee.preprocessing_for_ml(costs, regular_list[0:0], start_date)

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
        regular_events = ee.get_regular_events(regular_list, costs, test_date, test_date + relativedelta(months=1)).set_index('date')
        # Работаем толбко с затратами
        regular_events = regular_events[regular_events['amount']<0]

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
