import DataLoader as dl
import EventEngine as ee
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

class User:
    def __init__(self, id):
        self.id = id
        self.loaded = False

    def load_from_file(self, db_engine, file_full_name, new_balance):
        self.costs = dl.tinkoff_file_parse(file_full_name, db_engine, self.id)
        self.costs['balance'] = ee.get_balance_past(new_balance, self.costs['amount'])
        self.costs = ee.get_all_costs(self.costs, db_engine, self.id)
        self.loaded = True
        ee.save_new_costs(self.costs, db_engine, self.id)
        return self.costs

    def load_from_bd(self, db_engine):
        self.costs = db_engine.download_costs(self.id)
        self.loaded = True
        return self.costs

    def predict_regular(self, db_engine, end_date):
        self.regular_list = ee.get_updated_regular(
            self.costs,
            db_engine.download_regular(self.id)
        )

        self.predicted_regular = ee.predict_regular_events(self.regular_list, self.costs, end_date)
        return self.predicted_regular

    def predict_full(self, db_engine, end_date, start_date=datetime(2020,11,21)):
        return ee.get_full_costs(
            self.predicted_regular,
            ee.preprocessing_for_ml(self.costs, self.regular_list, start_date),
            self.costs['balance'].iloc[-1],
            db_engine.download_last_model(self.id),
            end_date
        )
        


class User_manager:
    def __init__(self, db_settings):
        self.db_engine = dl.DB_Engine(**db_settings)
        self.user_list = []

    def get_user(self, user_id):
        for u in self.user_list:
            if u.id == user_id:
                return u
        # if not found:
        new_u = User(user_id)
        self.user_list.append(new_u)
        return new_u

    def load_from_file(self, user_id, file_full_name, new_balance):
        return self.get_user(user_id).load_from_file(self.db_engine, file_full_name, new_balance)

    def load_from_db(self, user_id):
        return self.get_user(user_id).load_from_bd(self.db_engine)

    def predict_regular(self, user_id, end_date):
        user = self.get_user(user_id)
        if user.loaded == False:
            user.load_from_bd(self.db_engine)

        return user.predict_regular(self.db_engine, end_date)

    def predict_full(self, user_id, end_date):
        user = self.get_user(user_id)
        if user.loaded == False:
            user.load_from_bd(self.db_engine)

        return user.predict_full(self.db_engine, end_date)